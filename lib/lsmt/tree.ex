defmodule LSMT.Tree do
  alias LSMT.{Bloom, Segment, Serde, Memtable, Wal}

  defstruct directory: nil,
            segments: [],
            serde: LSMT.Serde,
            threshold: 1_000_000,
            memtable: nil,
            bloom: %Bloom{}

  @doc """
  Opens an existing Tree directory or initializes a new Tree directory.
  """
  def new(directory, opts \\ []) do
    threshold = opts[:threshold] || 1_000_000

    tree =
      if File.dir?(directory) do
        memtable = load_memtable(directory)
        bloom = load_bloom(directory)
        segments = load_segments(directory)

        %__MODULE__{
          directory: directory,
          bloom: bloom,
          memtable: memtable,
          segments: segments,
          threshold: threshold
        }
      else
        %__MODULE__{
          directory: directory,
          segments: [],
          threshold: threshold,
          # sparsity_factor: opts[:sparsity_factor] || 100,
          bloom: %Bloom{},
          memtable: Memtable.new()
          # index: new_index()
        }
      end

    :ok = Wal.init(directory)

    tree
  end

  @doc """
  Puts a new `key` and `value` into the given `tree`.

  ## Examples

      iex> tree = Tree.new(Briefly.create!(directory: true))
      iex> tree = Tree.put(tree, "hello", "world")
      iex> Tree.fetch(tree, "hello")
      {:ok, "world"}

  """
  def put(%__MODULE__{} = tree, key, value) do
    bin_key = Serde.ser(key)
    bin_value = Serde.ser(value)
    dir = directory(tree)
    memtable = memtable(tree)

    line = build_line(bin_key, bin_value)
    :ok = Wal.write(dir, line)
    :ok = Memtable.put(memtable, bin_key, bin_value)

    if is_above_threshold?(tree) do
      tree = flush(tree)
      :ok = Wal.rm(dir)
      :ok = Wal.init(dir)

      %__MODULE__{
        tree
        | memtable: Memtable.new()
      }
    else
      tree
    end
  end

  @doc """
  Fetches a `key` from the `tree`.

  ## Examples

      iex> tree = Tree.new(Briefly.create!(directory: true))
      iex> Tree.fetch(tree, "hello")
      :error
      iex> tree = Tree.put(tree, "hello", "world")
      iex> Tree.fetch(tree, "hello")
      {:ok, "world"}

  """
  def fetch(%__MODULE__{} = tree, key) do
    bin_key = Serde.ser(key)

    case Memtable.fetch(tree.memtable, bin_key) do
      {:ok, value} ->
        {:ok, Serde.de(value)}

      :error ->
        if Bloom.member?(tree.bloom, bin_key) do
          fetch_from_segments(tree, tree.segments, bin_key)
        else
          :error
        end
    end
  end

  defp fetch_from_segments(tree, [segment_num | rest_segments], bin_key) do
    tree
    |> segment_path(segment_num)
    |> File.open([:read], fn file ->
      case find_line(file, bin_key) do
        :eof ->
          :eof

        line when is_binary(line) ->
          split_value(line)
      end
    end)
    |> case do
      {:ok, :eof} ->
        fetch_from_segments(tree, rest_segments, bin_key)

      {:ok, value} when is_binary(value) ->
        {:ok, Serde.de(value)}
    end
  end

  defp fetch_from_segments(_tree, [], _bin_key) do
    :error
  end

  defp find_line(file, bin_key) do
    case IO.read(file, :line) do
      line when is_binary(line) ->
        if String.starts_with?(line, bin_key) do
          line
        else
          find_line(file, bin_key)
        end

      :eof ->
        :eof
    end
  end

  def is_above_threshold?(tree) do
    memtable_byte_size(tree) > threshold(tree)
  end

  def threshold(%__MODULE__{threshold: t}), do: t

  def memtable_byte_size(tree) do
    tree
    |> memtable()
    |> Memtable.byte_size()
  end

  defp build_line(bin_key, bin_value) do
    [bin_key, ":", bin_value, "\n"]
  end

  defp load_segments(directory) do
    if File.dir?(directory) do
      directory
      |> File.ls!()
      |> Enum.filter(fn f -> Path.extname(f) == ".data" end)
      |> Enum.map(fn f -> Segment.num(f) end)
    else
      []
    end
  end

  defp load_memtable(directory, memtable \\ Memtable.new()) do
    :ok = Wal.init(directory)

    directory
    |> Wal.lines()
    |> Stream.map(fn line ->
      {key, value} = split_line(line)
      Memtable.put(memtable, key, value)
    end)
    |> Stream.run()

    memtable
  end

  defp load_bloom(directory) when is_binary(directory) do
    directory
    |> File.ls!()
    |> Enum.reduce(%Bloom{}, fn filename, bloom ->
      if Segment.file?(filename) do
        directory
        |> Path.join(filename)
        |> File.stream!()
        |> Enum.reduce(bloom, fn line, bloom ->
          key = split_key(line)
          Bloom.put(bloom, key)
        end)
      else
        bloom
      end
    end)
  end

  @doc """
  Flushes the memtable to disk (into a segment file).
  """
  def flush(%__MODULE__{} = tree) do
    new_segment = current_segment_num(tree) + 1

    entries =
      tree
      |> memtable
      |> Memtable.to_list()

    iodata =
      Enum.map(entries, fn {key, value} ->
        build_line(key, value)
      end)

    bloom =
      Enum.reduce(entries, tree.bloom, fn {key, _}, bloom ->
        Bloom.put(bloom, key)
      end)

    segment_path = segment_path(tree, new_segment)

    :ok = File.write!(segment_path, iodata, [:create, :write, :sync])

    %__MODULE__{
      tree
      | segments: [new_segment | tree.segments],
        bloom: bloom,
        memtable: Memtable.new()
    }
  end

  def merge(%__MODULE__{} = tree, seg1, seg2) when seg1 < seg2 do
    path1 = segment_path(tree, seg1)
    path2 = segment_path(tree, seg2)
    new_path = path1 <> "temp"

    File.open(new_path, [:write], fn file0 ->
      File.open(path1, [:read], fn file1 ->
        File.open(path2, [:read], fn file2 ->
          merge_files(file0, file1, file2)
        end)
      end)
    end)

    File.rm!(path2)
    File.rm!(path1)
    File.rename(new_path, path1)

    %__MODULE__{tree | segments: tree.segments -- [seg2]}
  end

  def merge(tree, seg1, seg2) when seg1 == seg2 do
    raise """
    attempted to merge a segment with itself -
      segment: #{inspect(seg1)},
      directory: #{inspect(tree.directory)}
    """
  end

  def merge(tree, seg1, seg2) when seg1 > seg2 do
    raise """
    attempted to merge segments out of order -
      segment1: #{inspect(seg1)},
      segment2: #{inspect(seg2)},
      directory: #{inspect(tree.directory)}
    """
  end

  defp merge_files(file0, file1, file2) do
    line1 = IO.read(file1, :line)
    line2 = IO.read(file2, :line)
    merge_files(file0, file1, file2, line1, line2)
  end

  defp merge_files(file0, file1, file2, line1, line2) do
    case {split_key(line1), split_key(line2)} do
      {:eof, :eof} ->
        :done

      {_, :eof} ->
        :ok = IO.write(file0, line1)
        line1 = IO.read(file1, :line)
        merge_files(file0, file1, file2, line1, line2)

      {:eof, _} ->
        :ok = IO.write(file0, line2)
        line2 = IO.read(file2, :line)
        merge_files(file0, file1, file2, line1, line2)

      {k1, k2} when k1 == k2 ->
        # should we write line1 or line2?
        # defaulting to line1 for now, but it's probably not right...
        # TODO: figure out which line should be written and which should be discarded
        :ok = IO.write(file0, line2)
        line1 = IO.read(file1, :line)
        line2 = IO.read(file2, :line)
        merge_files(file0, file1, file2, line1, line2)

      {k1, k2} when k1 > k2 ->
        :ok = IO.write(file0, line2)
        line2 = IO.read(file2, :line)
        merge_files(file0, file1, file2, line1, line2)

      {k1, k2} when k1 < k2 ->
        :ok = IO.write(file0, line1)
        line1 = IO.read(file1, :line)
        merge_files(file0, file1, file2, line1, line2)
    end
  end

  defp split_line(line) do
    [key, value] = String.split(line, ":")
    value = String.trim_trailing(value, "\n")
    {key, value}
  end

  defp split_value(line) do
    [_, value] = String.split(line, ":")
    String.trim_trailing(value, "\n")
  end

  defp split_key(line) when is_binary(line) do
    [key, _value] = String.split(line, ":")
    key
  end

  defp split_key(:eof) do
    :eof
  end

  @doc """
  The directory of the Tree.

  All db data is stored in the directory.
  """
  def directory(%__MODULE__{directory: d}), do: d

  @doc """
  The memtable of the `tree`.
  """
  def memtable(%__MODULE__{memtable: m}), do: m

  defp current_segment_num(%__MODULE__{segments: []}), do: 0
  defp current_segment_num(%__MODULE__{segments: [n | _]}) when is_integer(n), do: n

  @doc """
  Returns the filepath of the given segment number.
  """
  def segment_path(tree, num) when is_integer(num) do
    filepath(tree, Segment.filename(num))
  end

  defp filepath(%__MODULE__{} = tree, filename) do
    tree
    |> directory()
    |> filepath(filename)
  end

  defp filepath(directory, filename) when is_binary(directory) do
    Path.join(directory, filename)
  end
end
