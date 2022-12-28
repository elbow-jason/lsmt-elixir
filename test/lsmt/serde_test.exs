defmodule LSMT.SerdeTest do
  use ExUnit.Case
  alias LSMT.Serde

  doctest Serde

  test "deserializing a bad float returns an error" do
    v = <<?f, 255, 255, 255, 255, 255, 255, 255, 255>>
    err = Serde.de_many(v)

    assert err ==
             {:error,
              [
                type: :float,
                reason: :invalid_encoding,
                binary: <<255, 255, 255, 255, 255, 255, 255, 255>>
              ]}
  end

  test "can deserialize partials" do
    ser = Serde.ser(1.0)
    <<partial::binary-size(4), rest::binary>> = IO.iodata_to_binary(ser)
    assert Serde.de_many(partial) == {:ok, [], partial}
    assert Serde.de_many(partial <> rest) == {:ok, [1.0], ""}
  end

  describe "de_one/1" do
    test "handles unknown tags" do
      assert Serde.de_one(<<?q, 0, 0, 0>>) == {:error, [type: :unknown, tag: "q"]}
    end

    test "handles partials" do
      ser = Serde.ser(1.0)
      <<partial::binary-size(4), _rest::binary>> = IO.iodata_to_binary(ser)
      assert Serde.de_one(partial) == :partial
    end

    test "works for floats" do
      ser = Serde.ser(1.0)
      assert Serde.de_one(ser) == {:ok, 1.0, ""}
    end

    test "works for integers" do
      ser = Serde.ser(1)
      assert Serde.de_one(ser) == {:ok, 1, ""}
    end

    test "works for atoms" do
      ser = Serde.ser(:name)
      assert Serde.de_one(ser) == {:ok, :name, ""}
    end

    test "works for strings" do
      ser = Serde.ser("some string")
      assert Serde.de_one(ser) == {:ok, "some string", ""}
    end

    test "handles non-exisiting atoms gracefully" do
      ser = <<?a, 0, 7>> <> "$$$$$$$"

      assert Serde.de_one(ser) ==
               {:error, [type: :atom, reason: :atom_does_not_exist, string: "$$$$$$$"]}
    end
  end
end
