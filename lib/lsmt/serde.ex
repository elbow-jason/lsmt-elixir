defmodule LSMT.Serde do
  @doc """
  Serializes a term into its binary format.

  ## Examples

      iex> Serde.ser(1)
      [105, <<0, 0, 0, 0, 0, 0, 0, 1>>]

      iex> Serde.ser(1.0)
      [102, <<191, 240, 0, 0, 0, 0, 0, 0>>]

      iex> Serde.ser(:name)
      [97, <<0, 4>>, "name"]

      iex> Serde.ser("elbow")
      [115, <<0, 0, 0, 5>>, "elbow"]
  """
  @spec ser(atom | binary | number) :: iodata()
  def ser(v) when is_float(v) do
    {:ok, bin} = ByteOrderedFloat.encode(v)
    [?f, bin]
  end

  def ser(v) when is_atom(v) do
    bin = Atom.to_string(v)
    n = byte_size(bin)
    size = <<n::big-unsigned-integer-size(16)>>
    [?a, size, bin]
  end

  def ser(v) when is_integer(v) do
    [?i, <<v::big-signed-integer-size(64)>>]
  end

  def ser(v) when is_binary(v) do
    n = byte_size(v)
    size = <<n::big-unsigned-integer-size(32)>>
    [?s, size, v]
  end

  # def ser_type(v) when is_float(v), do: :float
  # def ser_type(v) when is_integer(v), do: :integer
  # def ser_type(v) when is_atom(v), do: :atom
  # def ser_type(v) when is_binary(v), do: :string

  def de_one(iodata) when is_list(iodata) do
    iodata
    |> IO.iodata_to_binary()
    |> de_one()
  end

  def de_one(bin) when is_binary(bin) do
    with(
      {:ok, type, byte_len, data} <- de_parts(bin),
      {:ok, term, rest_data} <- do_de(type, byte_len, data)
    ) do
      {:ok, term, rest_data}
    end
  end

  def de_many(bin) do
    de_many(bin, [])
  end

  defp de_many(bin, acc) do
    case de_one(bin) do
      {:ok, term, rest} ->
        de_many(rest, [term | acc])

      :partial ->
        {:ok, Enum.reverse(acc), bin}

      :done ->
        {:ok, Enum.reverse(acc), ""}

      {:error, _} = err ->
        err
    end
  end

  @tags [?f, ?i, ?a, ?s]

  defp de_parts(""), do: :done
  defp de_parts(<<?f, data::binary>>), do: {:ok, :float, 8, data}
  defp de_parts(<<?i, data::binary>>), do: {:ok, :integer, 8, data}
  defp de_parts(<<?a, l::big-unsigned-integer-16, data::binary>>), do: {:ok, :atom, l, data}
  defp de_parts(<<?s, l::big-unsigned-integer-32, data::binary>>), do: {:ok, :string, l, data}
  # defp de_parts(<<tag, _::binary>>) when tag in @tags, do: :partial
  defp de_parts(<<tag, _::binary>>), do: {:error, [type: :unknown, tag: <<tag>>]}

  defp do_de(type, term_len, data) do
    data_len = byte_size(data)
    partial_check = if data_len < term_len, do: :partial, else: :ok

    with(
      :ok <- partial_check,
      term_bin = binary_part(data, 0, term_len),
      rest_len = data_len - term_len,
      rest_bin = binary_part(data, term_len, rest_len),
      {:ok, term} <- de_type(type, term_bin)
    ) do
      {:ok, term, rest_bin}
    end
  end

  defp de_type(:float, bin) when is_binary(bin) do
    case ByteOrderedFloat.decode(bin) do
      {:ok, f} ->
        {:ok, f}

      :error ->
        {:error, [type: :float, reason: :invalid_encoding, binary: bin]}
    end
  end

  defp de_type(:integer, <<int::big-signed-integer-size(64)>>) do
    {:ok, int}
  end

  defp de_type(:atom, string) when is_binary(string) do
    atom = String.to_existing_atom(string)
    {:ok, atom}
  rescue
    ArgumentError ->
      {:error, [type: :atom, reason: :atom_does_not_exist, string: string]}
  end

  defp de_type(:string, bin) when is_binary(bin) do
    # TODO: check for string uft8ness
    {:ok, bin}
  end
end
