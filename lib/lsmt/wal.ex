defmodule LSMT.Wal do
  @moduledoc """
  The LSMT write-ahead log.
  """

  alias LSMT.Utils

  @doc """
  Initializes a wal file into the given `directory`.


  ## Examples

      iex> dir = Briefly.create!(directory: true)
      iex> File.exists?(Wal.path(dir))
      false
      iex> Wal.init(dir)
      :ok
      iex> File.exists?(Wal.path(dir))
      true

  """
  def init(directory) do
    directory
    |> path()
    |> Utils.ensure_file_exists!()
  end

  @doc """
  Removes the wal from disk - use with care.

  ## Examples

      iex> dir = Briefly.create!(directory: true)
      iex> Wal.init(dir)
      :ok
      iex> File.exists?(Wal.path(dir))
      true
      iex> Wal.rm(dir)
      :ok
      iex> File.exists?(Wal.path(dir))
      false

  """
  def rm(directory) do
    p = path(directory)
    :ok = File.rm!(p)
    :ok
  end

  @doc """
  Returns the path the wal file in the given `directory`.

  ## Examples

      iex> Wal.path("/some/path/here")
      "/some/path/here/db.wal"

  """
  def path(directory) when is_binary(directory) do
    Path.join(directory, "db.wal")
  end

  @doc """
  Writes a line to the wal.

  ## Examples

      iex> dir = Briefly.create!(directory: true)
      iex> :ok = Wal.write(dir, "hello:world")
      iex> File.read!(Wal.path(dir))
      "hello:world"

  """
  def write(directory, line) do
    directory
    |> path()
    |> File.write!(line, [:append, :sync])
  end

  @doc """
  Streams the lines of the wal.
  """
  def lines(directory) do
    directory
    |> path()
    |> File.stream!()
  end
end
