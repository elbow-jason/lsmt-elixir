defmodule LSMT.SegmentTest do
  use ExUnit.Case
  alias LSMT.Segment

  doctest Segment

  describe "ensure_exists/2" do
    test "raises for an invalid segment filename" do
      dir = Briefly.create!(directory: true)

      err =
        assert_raise(RuntimeError, fn ->
          Segment.ensure_exists(dir, "some-filename.thing")
        end)

      assert err.message =~ "invalid segment filename"
    end
  end

  describe "num/1" do
    test "raises for an invalid segment filename" do
      err =
        assert_raise(RuntimeError, fn ->
          Segment.num("some-filename.thing")
        end)

      assert err.message =~ "invalid segment filename"
    end
  end
end
