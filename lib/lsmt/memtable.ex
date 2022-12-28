defmodule LSMT.Memtable do
  @type t :: :ets.tid() | atom()

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
  @spec to_list(t) :: [{term(), term()}]
  def to_list(memtable) do
    :ets.tab2list(memtable)
  end

  @doc """
  Reduces over the memtable.

  ## Examples

      iex> tab = Memtable.new()
      iex> :ok = Memtable.put(tab, "one", 1)
      iex> :ok = Memtable.put(tab, "two", 2)
      iex> Memtable.reduce(tab, 0, fn {_, n}, acc -> n + acc end)
      3
  """
  @spec reduce(t(), any, (any, any -> any)) :: any
  def reduce(memtable, acc, reducer) do
    :ets.foldl(reducer, acc, memtable)
  end
end
