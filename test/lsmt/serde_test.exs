defmodule LSMT.SerdeTest do
  use ExUnit.Case
  alias LSMT.Serde

  doctest Serde

  test "deserializing a bad float raises a RuntimeError" do
    v = "f" <> Base.encode64("bad_value", padding: false)

    err =
      assert_raise(RuntimeError, fn ->
        Serde.de(v)
      end)

    assert err.message =~ "invalid float encoding"
  end
end
