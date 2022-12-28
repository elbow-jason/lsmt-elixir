defmodule LSMT.Segment do
  @moduledoc """
  A sorted-on-disk collection of db entries.
  """
  alias LSMT.Utils

  @prefix "segment-"
  @ext ".data"

  @doc """
  Checks for valid segment file names and paths.

  This function does not check the existence or validity of the
  contents of a segment file; it only checks the filename format.

  ## Examples

      iex> Segment.file?("segment-11.data")
      true

      iex> Segment.file?("priv/data/segment-11.data")
      true

      iex> Segment.file?("nope")
      false

      iex> Segment.file?(:nope)
      false
  """
  @spec file?(path :: term) :: boolean
  def file?(path) when is_binary(path) do
    filename = Path.basename(path)
    String.starts_with?(filename, @prefix) and Path.extname(filename) == @ext
  end

  def file?(_), do: false

  @doc """
  Extracts the segment number from a filename or path.

  ## Examples

      iex> Segment.num("segment-11.data")
      11
  """
  @spec num(binary) :: integer
  def num(path) when is_binary(path) do
    path
    |> Path.basename()
    |> case do
      @prefix <> rest ->
        rest
        |> String.trim_trailing(@ext)
        |> String.to_integer()

      got ->
        raise "invalid segment filename: #{got}"
    end
  end

  @doc """
  Renders a segment number into a filename.

  ## Examples

      iex> Segment.filename(11)
      "segment-11.data"
  """
  @spec filename(integer) :: binary()
  def filename(num) when is_integer(num) do
    @prefix <> Integer.to_string(num) <> @ext
  end

  @doc """
  Ensures a segment file exists. If the segment file does not already exist, it is created via touch.

  ## Examples

      iex> dir = Briefly.create!(directory: true)
      iex> File.exists?(Path.join(dir, "segment-11.data"))
      false
      iex> Segment.ensure_exists(dir, 11)
      iex> File.exists?(Path.join(dir, "segment-11.data"))
      true

      iex> dir = Briefly.create!(directory: true)
      iex> File.exists?(Path.join(dir, "segment-11.data"))
      false
      iex> Segment.ensure_exists(dir, "segment-11.data")
      iex> File.exists?(Path.join(dir, "segment-11.data"))
      true
  """
  @spec ensure_exists(binary, binary | integer) :: binary
  def ensure_exists(directory, segment) when is_integer(segment) do
    fname = filename(segment)
    unchecked_ensure_exists(directory, fname)
  end

  def ensure_exists(directory, fname) when is_binary(fname) do
    # checks the segment filename format
    _ = num(fname)
    unchecked_ensure_exists(directory, fname)
  end

  defp unchecked_ensure_exists(directory, fname) do
    path = Path.join(directory, fname)
    :ok = Utils.ensure_file_exists!(path)
    path
  end
end
