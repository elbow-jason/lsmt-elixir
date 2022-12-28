defmodule LSMT.Serde do
  @moduledoc """
  Serialization and deserialization of keys and values.

  Currently supported types: 64bit float, signed 64bit integer, atom, string.

  # TODO: make serde configurable at runtime.
  """

  @doc """
  Serializes terms into their binary format.

  ## Examples

      iex> Serde.ser(1)
      "iAAAAAAAAAAE"

      iex> Serde.ser(1.0)
      "fv/AAAAAAAAA"

      iex> Serde.ser(:name)
      "abmFtZQ"

      iex> Serde.ser("elbow")
      "sZWxib3c"
  """
  @spec ser(atom | binary | number) :: binary
  def ser(v) do
    {tag, bin} = tag_and_bin(v)
    bin64 = Base.encode64(bin, padding: false)
    tag <> bin64
  end

  @doc """
  Deserializes binaries into an arbitrary Erlang/Elixir term.

  ## Examples

      iex> Serde.de("iAAAAAAAAAAE")
      1

      iex> Serde.de("fv/AAAAAAAAA")
      1.0

      iex> Serde.de("abmFtZQ")
      :name

      iex> Serde.de("sZWxib3c")
      "elbow"

  """
  @spec de(serialized_term :: binary) :: atom | binary | number
  def de(serialized_term)

  def de("f" <> bin) do
    bin
    |> Base.decode64!(padding: false)
    |> ByteOrderedFloat.decode()
    |> case do
      {:ok, f} ->
        f

      err ->
        raise """
        invalid float encoding -
          binary: #{inspect(bin, binaries: :as_binaries)}
          reason: #{inspect(err)}
        """
    end
  end

  def de("a" <> b64) do
    str = Base.decode64!(b64, padding: false)
    String.to_existing_atom(str)
  end

  def de(<<"i", b64::binary>>) do
    <<v::big-signed-integer-size(64)>> = Base.decode64!(b64, padding: false)
    v
  end

  def de(<<"s", b64::binary>>) do
    Base.decode64!(b64, padding: false)
  end

  defp tag_and_bin(v) when is_float(v) do
    {:ok, bin} = ByteOrderedFloat.encode(v)
    {"f", bin}
  end

  defp tag_and_bin(v) when is_atom(v) do
    {"a", Atom.to_string(v)}
  end

  defp tag_and_bin(v) when is_integer(v) do
    {"i", <<v::big-integer-size(64)>>}
  end

  defp tag_and_bin(v) when is_binary(v) do
    {"s", v}
  end
end
