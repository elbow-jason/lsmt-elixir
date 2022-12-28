defmodule LSMT.Utils do
  @doc """
  Creates a new file even if it's in a new directory.

  ## Examples

      iex> tmp_dir = Briefly.create!(directory: true)
      iex> dir = Path.join(tmp_dir, "things")
      iex> File.exists?(dir) || File.dir?(dir)
      false
      iex> fpath = Path.join(dir, "file.txt")
      iex> File.exists?(fpath)
      false
      iex> Utils.ensure_file_exists!(fpath)
      iex> File.dir?(dir)
      true
      iex> File.exists?(fpath)
      true
  """
  def ensure_file_exists!(path) when is_binary(path) do
    dir = Path.dirname(path)
    :ok = File.mkdir_p!(dir)

    if File.exists?(path) do
      :ok
    else
      :ok = File.touch!(path)
      :ok
    end
  end
end
