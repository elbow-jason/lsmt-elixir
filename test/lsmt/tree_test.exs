defmodule LSMT.TreeTest do
  use ExUnit.Case

  alias LSMT.{Bloom, Memtable, Segment, Serde, Tree}

  doctest Tree

  def tmp_dir do
    Briefly.create!(directory: true)
  end

  describe "new/2" do
    test "returns a new tree" do
      dir = tmp_dir()
      tree = Tree.new(dir)
      # new bloom
      assert tree.bloom == %Bloom{}

      # no segments for a brand new tree
      assert tree.segments == []

      # directory is the same
      assert tree.directory == dir

      # ets table is new
      assert is_reference(tree.memtable)
      assert :ets.info(tree.memtable, :size) == 0

      # threshold is the default
      assert Tree.threshold(tree) == 1_000_000
    end

    test "initializes wal file" do
      dir = tmp_dir()
      _tree = Tree.new(dir)
      assert Enum.sort(File.ls!(dir)) == ["db.wal"]
      assert File.stat!(Path.join(dir, "db.wal")).size == 0
    end

    test "creates a new directory" do
      dir = Path.join(tmp_dir(), "some/new_dir/")
      _tree = Tree.new(dir)
      assert File.exists?(dir) and File.dir?(dir)
    end

    test "can load an existing tree correctly" do
      tree =
        tmp_dir()
        |> Tree.new()
        |> Tree.put("hello1", "world1")
        |> Tree.flush()
        |> Tree.put("hello2", "world2")
        |> Tree.flush()
        |> Tree.put("hello3", "world3")
        |> Tree.flush()
        |> Tree.put("hello4", "world4")
        |> Tree.flush()
        |> Tree.put("hello5", "world5")

      dir = Tree.directory(tree)

      tree2 = Tree.new(dir)
      assert tree.bloom == tree2.bloom
      assert tree |> Tree.memtable() |> Memtable.to_list() == [{"hello5", "world5"}]
    end
  end

  describe "put/3" do
    test "called twice with the same key retains the newer value" do
      dir = tmp_dir()
      tree = Tree.new(dir)
      tree = Tree.put(tree, "count", 1)
      tree = Tree.put(tree, "count", 2)
      assert {:ok, 2} == Tree.fetch(tree, "count")
    end

    test "causes a flush when size goes over the threshold" do
      dir = tmp_dir()
      tree = Tree.new(dir, threshold: 24)
      assert tree.threshold == 24
      tree = Tree.put(tree, "hello", "world")
      assert Tree.memtable_byte_size(tree) == 14
      table_before = Tree.memtable(tree)
      assert tree.segments == []
      tree = Tree.put(tree, "hello_there_beautiful", "worlds_apart")
      assert Tree.memtable_byte_size(tree) == 0
      table_after = Tree.memtable(tree)
      assert table_before != table_after
      assert tree.segments == [1]

      content =
        dir
        |> Path.join(Segment.filename(1))
        |> File.read!()

      assert content ==
               "s\0\0\0\x05hellos\0\0\0\x05worlds\0\0\0\x15hello_there_beautifuls\0\0\0\fworlds_apart"
    end
  end

  describe "fetch/2" do
    test "returns :error for non-keys" do
      dir = tmp_dir()
      tree = Tree.new(dir)
      assert Tree.fetch(tree, "nope") == :error
    end

    test "works before flushing" do
      dir = tmp_dir()
      tree = Tree.new(dir)
      tree = Tree.put(tree, "hello", "world")
      assert {:ok, "world"} == Tree.fetch(tree, "hello")
    end

    test "works after flushing" do
      tree =
        tmp_dir()
        |> Tree.new()
        |> Tree.put("hello1", "world1")
        |> Tree.flush()
        |> Tree.put("hello2", "world2")
        |> Tree.flush()
        |> Tree.put("hello3", "world3")
        |> Tree.flush()
        |> Tree.put("hello4", "world4")
        |> Tree.flush()
        |> Tree.put("hello5", "world5")

      # fetch from memtable
      assert Tree.fetch(tree, "hello5") == {:ok, "world5"}

      # fetch from segments
      assert Tree.fetch(tree, "hello4") == {:ok, "world4"}
      assert Tree.fetch(tree, "hello3") == {:ok, "world3"}
      assert Tree.fetch(tree, "hello2") == {:ok, "world2"}
      assert Tree.fetch(tree, "hello1") == {:ok, "world1"}
    end

    test "works even with bloom filter collisions" do
      tree =
        tmp_dir()
        |> Tree.new()
        |> Tree.put("some", "thing")
        |> Tree.flush()

      # set up a fake collision because collision is not easy to *actually* cause.
      bloom = Bloom.put(tree.bloom, "hello")
      tree = %Tree{tree | bloom: bloom}
      assert Tree.fetch(tree, "hello") == :error
    end
  end

  describe "flush/1" do
    test "creates a segment on disk" do
      dir = tmp_dir()
      tree = Tree.new(dir)
      tree = Tree.put(tree, "hello", "world")
      tree = Tree.flush(tree)
      assert File.ls!(dir) == ["segment-1.data", "db.wal"]
      assert :ets.tab2list(tree.memtable) == []
      assert tree.segments == [1]

      content =
        dir
        |> Path.join(Segment.filename(1))
        |> File.read!()

      assert content == "s\0\0\0\x05hellos\0\0\0\x05world"
    end
  end

  describe "merge/3" do
    test "merges segments" do
      tree =
        tmp_dir()
        |> Tree.new()
        |> Tree.put("hello1", "world1")
        |> Tree.flush()
        |> Tree.put("hello2", "world2")
        |> Tree.put("hello3", "world3")
        |> Tree.flush()
        |> Tree.put("hello4", "world4")
        |> Tree.put("hello5", "world5")
        |> Tree.put("hello6", "world6")
        |> Tree.put("hello7", "world7")
        |> Tree.put("hello8", "world8")
        |> Tree.flush()

      assert tree.segments == [3, 2, 1]

      s1 = Tree.segment_path(tree, 1)
      s2 = Tree.segment_path(tree, 2)
      s3 = Tree.segment_path(tree, 3)

      assert File.read!(s1) == "s\0\0\0\x06hello1s\0\0\0\x06world1"

      assert File.read!(s2) ==
               "s\0\0\0\x06hello2s\0\0\0\x06world2s\0\0\0\x06hello3s\0\0\0\x06world3"

      assert File.read!(s3) ==
               "s\0\0\0\x06hello4s\0\0\0\x06world4s\0\0\0\x06hello5s\0\0\0\x06world5s\0\0\0\x06hello6s\0\0\0\x06world6s\0\0\0\x06hello7s\0\0\0\x06world7s\0\0\0\x06hello8s\0\0\0\x06world8"

      tree = Tree.merge(tree, 1, 2)
      assert tree.segments == [3, 1]
      assert File.exists?(s2) == false
      assert File.exists?(s1) == true

      assert File.read!(s1) ==
               "s\0\0\0\x06hello1s\0\0\0\x06world1s\0\0\0\x06hello2s\0\0\0\x06world2s\0\0\0\x06hello3s\0\0\0\x06world3"

      tree = Tree.merge(tree, 1, 3)
      assert tree.segments == [1]
      assert File.exists?(s3) == false
      assert File.exists?(s1) == true

      assert File.read!(s1) ==
               "s\0\0\0\x06hello1s\0\0\0\x06world1s\0\0\0\x06hello2s\0\0\0\x06world2s\0\0\0\x06hello3s\0\0\0\x06world3s\0\0\0\x06hello4s\0\0\0\x06world4s\0\0\0\x06hello5s\0\0\0\x06world5s\0\0\0\x06hello6s\0\0\0\x06world6s\0\0\0\x06hello7s\0\0\0\x06world7s\0\0\0\x06hello8s\0\0\0\x06world8"
    end

    test "retains sort-order even when segment are overlapping" do
      tree =
        tmp_dir()
        |> Tree.new()
        |> Tree.put("hello1", "world1")
        |> Tree.put("hello2", "world2")
        |> Tree.put("hello7", "world7")
        |> Tree.put("hello8", "world8")
        |> Tree.flush()
        |> Tree.put("hello3", "world3")
        |> Tree.put("hello4", "world4")
        |> Tree.put("hello5", "world5")
        |> Tree.put("hello6", "world6")
        |> Tree.flush()

      assert tree.segments == [2, 1]

      s1 = Tree.segment_path(tree, 1)
      s2 = Tree.segment_path(tree, 2)

      assert File.read!(s1) ==
               "s\0\0\0\x06hello1s\0\0\0\x06world1s\0\0\0\x06hello2s\0\0\0\x06world2s\0\0\0\x06hello7s\0\0\0\x06world7s\0\0\0\x06hello8s\0\0\0\x06world8"

      assert File.read!(s2) ==
               "s\0\0\0\x06hello3s\0\0\0\x06world3s\0\0\0\x06hello4s\0\0\0\x06world4s\0\0\0\x06hello5s\0\0\0\x06world5s\0\0\0\x06hello6s\0\0\0\x06world6"

      tree = Tree.merge(tree, 1, 2)

      assert tree.segments == [1]
      assert File.exists?(s2) == false
      assert File.exists?(s1) == true

      assert File.read!(s1) ==
               "s\0\0\0\x06hello1s\0\0\0\x06world1s\0\0\0\x06hello2s\0\0\0\x06world2s\0\0\0\x06hello3s\0\0\0\x06world3s\0\0\0\x06hello4s\0\0\0\x06world4s\0\0\0\x06hello5s\0\0\0\x06world5s\0\0\0\x06hello6s\0\0\0\x06world6s\0\0\0\x06hello7s\0\0\0\x06world7s\0\0\0\x06hello8s\0\0\0\x06world8"
    end

    test "keeps newer value when merging" do
      tree =
        tmp_dir()
        |> Tree.new()
        |> Tree.put("hello", "first")
        |> Tree.flush()
        |> Tree.put("hello", "second")
        |> Tree.flush()

      assert tree.segments == [2, 1]

      s1 = Tree.segment_path(tree, 1)
      s2 = Tree.segment_path(tree, 2)

      s1_data = File.read!(s1)
      assert s1_data == "s\0\0\0\x05hellos\0\0\0\x05first"
      assert Serde.de_many(s1_data) == {:ok, ["hello", "first"], ""}

      s2_data = File.read!(s2)

      assert s2_data == "s\0\0\0\x05hellos\0\0\0\x06second"
      assert Serde.de_many(s2_data) == {:ok, ["hello", "second"], ""}

      tree = Tree.merge(tree, 1, 2)
      assert tree.segments == [1]
      assert File.exists?(s2) == false
      assert File.exists?(s1) == true

      s1_data = File.read!(s1)
      assert s1_data == "s\0\0\0\x05hellos\0\0\0\x06second"
      assert Serde.de_many(s1_data) == {:ok, ["hello", "second"], ""}
    end

    test "raises for invalid merge order" do
      tree = Tree.new(tmp_dir())

      err =
        assert_raise(RuntimeError, fn ->
          Tree.merge(tree, 2, 1)
        end)

      assert err.message =~ "attempted to merge segments out of order"
    end

    test "raise for segment self-merging" do
      tree = Tree.new(tmp_dir())

      err =
        assert_raise(RuntimeError, fn ->
          Tree.merge(tree, 1, 1)
        end)

      assert err.message =~ "attempted to merge a segment with itself"
    end
  end
end
