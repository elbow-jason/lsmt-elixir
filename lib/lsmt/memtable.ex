defmodule LSMT.Memtable do
  @cfg [
    :ordered_set,
    :public,
    read_concurrency: true,
    write_concurrency: true
  ]

  @doc """
  A new ordered set ets table.
  """
  def new, do: :ets.new(nil, @cfg)

  @empty_memory_size nil
                     |> :ets.new(@cfg)
                     |> :ets.info(:memory)

  @doc """
  Computes the current memory size of the `memtable`.
  """
  def byte_size(memtable) do
    :ets.info(memtable, :memory) - @empty_memory_size
  end

  @doc """
  Puts a `key` and `value` into the `memtable`.

  Replaces old values with new values.
  """
  def put(memtable, key, value) do
    _ = :ets.insert(memtable, {key, value})
    :ok
  end

  @doc """
  Fetches a `key` from the `memtable`.

  Returns `{:ok, value}` when the `key` is found.
  Returns `:error` when the `key` is not found.

  ## Examples

      iex> t = Memtable.new()
      iex> :ok = Memtable.put(t, "hello", "world")
      iex> Memtable.fetch(t, "hello")
      {:ok, "world"}

      iex> t = Memtable.new()
      iex> Memtable.fetch(t, "nope")
      :error
  """
  def fetch(memtable, key) do
    case :ets.lookup(memtable, key) do
      [] -> :error
      [{^key, val}] -> {:ok, val}
    end
  end

  @doc """
  Returns the memtable as a list.
  """
  def to_list(memtable) do
    :ets.tab2list(memtable)
  end
end
