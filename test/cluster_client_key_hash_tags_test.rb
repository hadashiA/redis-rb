# frozen_string_literal: true

require_relative 'helper'

# ruby -w -Itest test/cluster_client_key_hash_tags_test.rb
class TestClusterClientKeyHashTags < Test::Unit::TestCase
  include Helper::Cluster

  def test_key_extraction
    described_class = Redis::Cluster::KeyExtractor

    assert_equal 'dogs:1', described_class.extract(%w[get dogs:1])
    assert_equal 'user1000', described_class.extract(%w[get {user1000}.following])
    assert_equal 'user1000', described_class.extract(%w[get {user1000}.followers])
    assert_equal 'foo{}{bar}', described_class.extract(%w[get foo{}{bar}])
    assert_equal '{bar', described_class.extract(%w[get foo{{bar}}zap])
    assert_equal 'bar', described_class.extract(%w[get foo{bar}{zap}])

    assert_equal '', described_class.extract([:get, ''])
    assert_equal '', described_class.extract([:get, nil])
    assert_equal '', described_class.extract([:get])

    assert_equal '', described_class.extract([:set, '', 1])
    assert_equal '', described_class.extract([:set, nil, 1])
    assert_equal '', described_class.extract([:set])

    # Keyless commands
    assert_equal '', described_class.extract([:auth, 'password'])
    assert_equal '', described_class.extract(%i[client kill])
    assert_equal '', described_class.extract(%i[cluster addslots])
    assert_equal '', described_class.extract(%i[command])
    assert_equal '', described_class.extract(%i[command count])
    assert_equal '', described_class.extract(%i[config get])
    assert_equal '', described_class.extract(%i[debug segfault])
    assert_equal '', described_class.extract([:echo, 'Hello World'])
    assert_equal '', described_class.extract([:flushall, 'ASYNC'])
    assert_equal '', described_class.extract([:flushdb, 'ASYNC'])
    assert_equal '', described_class.extract([:info, 'cluster'])
    assert_equal '', described_class.extract(%i[memory doctor])
    assert_equal '', described_class.extract([:ping, 'Hi'])
    assert_equal '', described_class.extract(%w[script exists sha1 sha1])
    assert_equal '', described_class.extract([:select, 1])
    assert_equal '', described_class.extract([:shutdown, 'SAVE'])
    assert_equal '', described_class.extract([:slaveof, '127.0.0.1', 6379])
    assert_equal '', described_class.extract([:slowlog, 'get', 2])
    assert_equal '', described_class.extract([:swapdb, 0, 1])
    assert_equal '', described_class.extract([:wait, 1, 0])

    # 2nd argument is not a key
    assert_equal 'key1', described_class.extract([:eval, 'script', 2, 'key1', 'key2', 'first', 'second'])
    assert_equal '', described_class.extract([:eval, 'return 0', 0])
    assert_equal 'key1', described_class.extract([:evalsha, 'sha1', 2, 'key1', 'key2', 'first', 'second'])
    assert_equal '', described_class.extract([:evalsha, 'return 0', 0])
    assert_equal 'key1', described_class.extract([:migrate, '127.0.0.1', 6379, 'key1', 0, 5000])
    assert_equal 'key1', described_class.extract([:memory, :usage, 'key1'])
    assert_equal 'key1', described_class.extract([:object, 'refcount', 'key1'])
    assert_equal 'mystream', described_class.extract([:xread, 'COUNT', 2, 'STREAMS', 'mystream', 0])
    assert_equal 'mystream', described_class.extract([:xreadgroup, 'GROUP', 'mygroup', 'Bob', 'COUNT', 2, 'STREAMS', 'mystream', '>'])
  end
end
