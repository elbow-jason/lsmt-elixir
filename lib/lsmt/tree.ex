defmodule LSMT.Tree do
  alias LSMT.{Bloom, Segment, Serde, Memtable, Streams, Wal}

  defstruct directory: nil,
            segments: [],
            serde: LSMT.Serde,
            threshold: 1_000_000,
            memtable: nil,
            bloom: %Bloom{}

  @type t :: %__MODULE__{
          directory: binary(),
          segments: list(non_neg_integer()),
          serde: module(),
          threshold: pos_integer(),
          memtable: Memtable.t(),
          bloom: Bloom.t()
        }
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
    dir = directory(tree)
    memtable = memtable(tree)

    line = build_line(key, value)
    :ok = Wal.write(dir, line)
    :ok = Memtable.put(memtable, key, value)

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
    case Memtable.fetch(tree.memtable, key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        if Bloom.member?(tree.bloom, key) do
          fetch_from_segments(tree, tree.segments, key)
        else
          :error
        end
    end
  end

  # def range(%__MODULE__{} = tree, lo, hi) when lo <= hi do
  #   key_matches? = fn key -> key >= lo and key <= hi end

  #   in_mems =
  #     Memtable.reduce(tree.memtable, [], fn {key, _} = entry, acc ->
  #       if key_matches?.(key) do
  #         [entry | acc]
  #       else
  #         acc
  #       end
  #     end)

  #   dir = directory(tree)

  #   in_segments =
  #     tree.segments
  #     |> Enum.map(fn seg ->
  #       fname = Segment.filename(seg)

  #       dir
  #       |> Path.join(fname)
  #       |> Utils.stream_file()
  #       |> Enum.filter(fn {key, _value} -> key_matches?.(key) end)
  #     end)

  #   sort_table = Memtable.new()

  #   [in_mems | in_segments]
  #   |> Enum.reverse()
  #   |> Enum.each(fn chunk ->
  #     Enum.each(chunk, fn {key, val} ->
  #       :ok = Memtable.put(sort_table, key, val)
  #     end)
  #   end)

  #   Memtable.to_list(sort_table)
  # end

  # def range(tree, hi, lo) when hi > lo do
  #   tree
  #   |> range(lo, hi)
  #   |> Enum.reverse()
  # end

  defp fetch_from_segments(tree, [segment_num | rest_segments], want_key) do
    tree
    |> segment_path(segment_num)
    |> Streams.stream_file()
    |> Stream.filter(fn {key, _} -> key == want_key end)
    |> Enum.take(1)
    |> case do
      [{^want_key, value}] ->
        {:ok, value}

      [] ->
        fetch_from_segments(tree, rest_segments, want_key)
    end
  end

  defp fetch_from_segments(_tree, [], _bin_key) do
    :error
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

  defp build_line(key, value) do
    ser_key = Serde.ser(key)
    ser_value = Serde.ser(value)
    [ser_key, ser_value]
  end

  defp load_segments(directory) do
    if File.dir?(directory) do
      directory
      |> File.ls!()
      |> Enum.filter(fn f -> Path.extname(f) == ".data" end)
      |> Enum.map(fn f -> Segment.num(f) end)
      |> Enum.sort(:desc)
    else
      []
    end
  end

  defp load_memtable(directory, memtable \\ Memtable.new()) do
    :ok = Wal.init(directory)

    :ok =
      directory
      |> Wal.path()
      |> Streams.stream_file()
      |> Enum.each(fn {key, value} -> Memtable.put(memtable, key, value) end)

    memtable
  end

  defp load_bloom(directory) when is_binary(directory) do
    directory
    |> File.ls!()
    |> Enum.reduce(%Bloom{}, fn filename, bloom ->
      if Segment.file?(filename) do
        directory
        |> Path.join(filename)
        |> Streams.stream_file()
        |> Enum.reduce(bloom, fn {key, _}, bloom ->
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
    segments = Enum.sort(tree.segments, :desc)

    new_segment =
      case segments do
        [] -> 1
        [max_seg | _] -> max_seg + 1
      end

    entries =
      tree
      |> memtable()
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
      | segments: [new_segment | segments],
        bloom: bloom,
        memtable: Memtable.new()
    }
  end

  @doc """
  Merges two segments of the tree into the first.

  Note: provided segments must be in order - providing out-of-order segment
  arguments will raise.
  """
  @spec merge(t(), integer, integer) :: t()
  def merge(%__MODULE__{} = tree, seg1, seg2) do
    if seg1 == seg2 do
      raise """
      attempted to merge a segment with itself -
        segment: #{inspect(seg1)},
        directory: #{inspect(tree.directory)}
      """
    end

    if seg1 > seg2 do
      raise """
      attempted to merge segments out of order -
        segment1: #{inspect(seg1)},
        segment2: #{inspect(seg2)},
        directory: #{inspect(tree.directory)}
      """
    end

    path1 = segment_path(tree, seg1)
    path2 = segment_path(tree, seg2)
    tmp_path = path1 <> "temp"

    File.open(tmp_path, [:write], fn tmp_file ->
      merger = LSMT.Streams.file_merger(path1, path2)

      Enum.each(merger, fn {key, value} ->
        line = build_line(key, value)
        :ok = IO.write(tmp_file, line)
      end)
    end)

    File.rm!(path2)
    File.rm!(path1)
    File.rename(tmp_path, path1)

    %__MODULE__{tree | segments: tree.segments -- [seg2]}
  end

  # defp merge_files(file0, stream1, stream2) do
  #   merge_files(file0, file1, file2, line1, line2)
  # end

  # defp merge_files(file0, file1, file2, line1, line2) do
  #   case {split_key(line1), split_key(line2)} do
  #     {:eof, :eof} ->
  #       :done

  #     {_, :eof} ->
  #       :ok = IO.write(file0, line1)
  #       line1 = IO.read(file1, :line)
  #       merge_files(file0, file1, file2, line1, line2)

  #     {:eof, _} ->
  #       :ok = IO.write(file0, line2)
  #       line2 = IO.read(file2, :line)
  #       merge_files(file0, file1, file2, line1, line2)

  #     {k1, k2} when k1 == k2 ->
  #       # should we write line1 or line2?
  #       # defaulting to line1 for now, but it's probably not right...
  #       # TODO: figure out which line should be written and which should be discarded
  #       :ok = IO.write(file0, line2)
  #       line1 = IO.read(file1, :line)
  #       line2 = IO.read(file2, :line)
  #       merge_files(file0, file1, file2, line1, line2)

  #     {k1, k2} when k1 > k2 ->
  #       :ok = IO.write(file0, line2)
  #       line2 = IO.read(file2, :line)
  #       merge_files(file0, file1, file2, line1, line2)

  #     {k1, k2} when k1 < k2 ->
  #       :ok = IO.write(file0, line1)
  #       line1 = IO.read(file1, :line)
  #       merge_files(file0, file1, file2, line1, line2)
  #   end
  # end

  # defp split_line(line) do
  #   [key, value] = String.split(line, ":")
  #   value = String.trim_trailing(value, "\n")
  #   {key, value}
  # end

  # defp split_value(line) do
  #   [_, value] = String.split(line, ":")
  #   String.trim_trailing(value, "\n")
  # end

  # defp split_key(line) when is_binary(line) do
  #   [key, _value] = String.split(line, ":")
  #   key
  # end

  # defp split_key(:eof) do
  #   :eof
  # end

  @doc """
  The directory of the Tree.

  All db data is stored in the directory.
  """
  def directory(%__MODULE__{directory: d}), do: d

  @doc """
  The memtable of the `tree`.
  """
  def memtable(%__MODULE__{memtable: m}), do: m

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
