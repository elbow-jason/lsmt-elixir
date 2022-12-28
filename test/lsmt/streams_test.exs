defmodule LSMT.StreamsTest do
  use ExUnit.Case
  alias LSMT.{Streams, Serde}

  doctest Streams

  describe "file_stream/1" do
    test "raises for invalid files" do
      filepath = Briefly.create!()
      data = List.duplicate("what", 10_000)
      :ok = File.write!(filepath, data)

      err =
        assert_raise(RuntimeError, fn ->
          filepath
          |> Streams.stream_file()
          |> Enum.into([])
        end)

      assert err.message =~ "encountered an error while streaming file"
    end

    test "raises for partial files" do
      iodata = [
        Serde.ser(1),
        Serde.ser(2),
        Serde.ser(3),
        Serde.ser(4)
      ]

      bin = IO.iodata_to_binary(iodata)
      assert byte_size(bin) == 36
      partial = binary_part(bin, 0, 30)
      filepath = Briefly.create!()
      :ok = File.write!(filepath, partial)

      err =
        assert_raise(RuntimeError, fn ->
          filepath
          |> Streams.stream_file(10)
          |> Enum.into([])
        end)

      assert err.message =~ "file is incomplete"
    end
  end
end
