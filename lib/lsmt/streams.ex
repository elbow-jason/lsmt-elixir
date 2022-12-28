defmodule LSMT.Streams do
  alias LSMT.Serde

  def file_merger(path1, path2, chunk_size \\ 4096) do
    Stream.resource(
      fn ->
        file1 = File.open!(path1, [:read])
        file2 = File.open!(path2, [:read])

        one = %{
          status: :cont,
          path: path1,
          file: file1,
          entries: [],
          data: ""
        }

        two = %{
          status: :cont,
          path: path2,
          file: file2,
          entries: [],
          data: ""
        }

        {one, two}
      end,
      fn {one, two} ->
        one = refill(one, chunk_size)
        two = refill(two, chunk_size)

        case pick_sorted(one, two) do
          {:ok, picked, one, two} ->
            {[picked], {one, two}}

          :halt ->
            {:halt, {one, two}}
        end
      end,
      fn {one, two} ->
        _ = File.close(one.file)
        _ = File.close(two.file)
      end
    )
  end

  defp refill(%{status: :halt} = state, _chunk_size) do
    state
  end

  defp refill(%{status: :cont, entries: [_ | _]} = state, _chunk_size) do
    state
  end

  defp refill(%{status: :cont, entries: [], file: file, data: data} = state, chunk_size) do
    case read_data_chunk(file, chunk_size, data) do
      {:ok, terms, data} ->
        entries = terms_to_entries(state.path, terms)
        %{state | entries: entries, data: data}

      {:halt, _file} ->
        %{state | status: :halt}
    end
  end

  defp pick_sorted(%{status: :halt}, %{status: :halt}) do
    :halt
  end

  defp pick_sorted(%{entries: e1} = one, %{entries: e2} = two) do
    {:ok, entry, e1, e2} = do_pick_sorted(e1, e2)

    one = %{one | entries: e1}
    two = %{two | entries: e2}
    {:ok, entry, one, two}
  end

  defp do_pick_sorted([{k1, v1} | rest1] = list1, [{k2, v2} | rest2] = list2) do
    case [] do
      _ when k1 == k2 ->
        # keep 2 and discard 1
        {:ok, {k2, v2}, rest1, rest2}

      _ when k1 < k2 ->
        {:ok, {k1, v1}, rest1, list2}

      _ when k1 > k2 ->
        {:ok, {k2, v2}, list1, rest2}
    end
  end

  defp do_pick_sorted([], [{k, v} | rest]) do
    {:ok, {k, v}, [], rest}
  end

  defp do_pick_sorted([{k, v} | rest], []) do
    {:ok, {k, v}, rest, []}
  end

  def stream_file(path, chunk_size \\ 4096) do
    Stream.resource(
      fn -> %{file: File.open!(path, [:read]), data: ""} end,
      fn %{file: file, data: prev} = state ->
        case read_data_chunk(file, chunk_size, prev) do
          {:ok, terms, data} ->
            entries = terms_to_entries(path, terms)
            {entries, %{file: file, data: data}}

          {:halt, _file} ->
            {:halt, state}

          {:error, _} = err ->
            raise """
            encountered an error while streaming file -
            filepath: #{inspect(path)}
            error: #{inspect(err)}
            """
        end
      end,
      fn file -> File.close(file) end
    )
  end

  defp terms_to_entries(path, terms) do
    terms
    |> Enum.chunk_every(2)
    |> Enum.map(fn
      [k, v] ->
        {k, v}

      [_] ->
        raise "file is incomplete: #{inspect(path)}"
    end)
  end

  defp read_data_chunk(file, chunk_size, prev) do
    case IO.read(file, chunk_size) do
      bin when is_binary(bin) ->
        case Serde.de_many(prev <> bin) do
          {:ok, terms, rest} ->
            {:ok, terms, rest}

          {:error, _} = err ->
            err
        end

      :eof ->
        {:halt, file}
    end
  end
end
