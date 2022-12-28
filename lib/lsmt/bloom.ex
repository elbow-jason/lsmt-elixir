defmodule LSMT.Bloom do
  @moduledoc """
  A bare-bones bloom filter that uses integers and bitshifting.

  ## Hash Algorithms

  Currently LSMT.Bloom is using `:erlang.phash/1` and `Murmur.hash_x64_128/1`
  as it's default hashes.

  """
  import Bitwise

  @derive {Inspect, only: [:size, :capacity, :hashers]}

  @default_capacity 4096

  @default_hashers [{:erlang, :phash2}, {Murmur, :hash_x64_128}]

  defstruct size: 0,
            bits: 0,
            capacity: @default_capacity,
            hashers: @default_hashers

  @type t :: %__MODULE__{
          size: non_neg_integer(),
          bits: non_neg_integer(),
          capacity: non_neg_integer(),
          hashers: [{module(), atom()}]
        }
  @doc """
  Puts a value (usually the key of a LSM entry) into the bloom filter.

  ## Examples

      iex> b = Bloom.put(%Bloom{}, "thing")
      iex> Bloom.member?(b, "thing")
      true
      iex> Bloom.member?(b, "not thing")
      false
  """
  def put(%__MODULE__{bits: bits, size: size} = bloom, value) do
    idxs = apply_hashers(bloom, value)
    %__MODULE__{bloom | bits: set_bits(bits, idxs), size: size + 1}
  end

  @doc """
  Returns the number of entries in the bloom filter.

  Note: the bloom does not keep track of repeatedly added values; calling
  `put/2` will result in Bloom size that is larger even if the value that is
  being `put`ted has already been `put`ted.

  ## Examples

    iex> b = %Bloom{}
    iex> Bloom.size(b)
    0
    iex> b = Bloom.put(b, "thing")
    iex> Bloom.size(b)
    1
    iex> b = Bloom.put(b, "thing")
    iex> Bloom.size(b)
    2
  """
  def size(%__MODULE__{size: s}), do: s

  @doc """
  Returns true if the given value is in the bloom filter.

  Note: `true` results may be false-positives, but `false` results are true-negatives.

  ## Examples

      iex> b = Bloom.put(%Bloom{}, "thing")
      iex> Bloom.member?(b, "thing")
      true

      iex> Bloom.member?(%Bloom{}, "some_key")
      false
  """
  def member?(%__MODULE__{bits: bits} = bloom, value) do
    idxs = apply_hashers(bloom, value)
    Enum.all?(idxs, fn i -> has_bit?(bits, i) end)
  end

  defp apply_hashers(%__MODULE__{hashers: hashers, capacity: capacity}, value) do
    Enum.map(hashers, fn {module, func} ->
      hash = apply(module, func, [value])
      rem(hash, capacity)
    end)
  end

  defp set_bits(bits, []), do: bits
  defp set_bits(bits, [i | rest]), do: set_bits(bits ||| 1 <<< i, rest)

  defp has_bit?(bits, i), do: (bits &&& 1 <<< i) > 0
end
