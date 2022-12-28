defmodule LSMT.BloomTest do
  use ExUnit.Case
  alias LSMT.Bloom

  doctest Bloom

  test "bits are not rendered in Bloom's  implementation of Inspect protocol" do
    ins = inspect(%Bloom{})
    refute ins =~ "bits"
  end
end
