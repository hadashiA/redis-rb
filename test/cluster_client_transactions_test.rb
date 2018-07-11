# frozen_string_literal: true

require_relative 'helper'

# ruby -w -Itest test/cluster_client_transactions_test.rb
class TestClusterClientTransactions < Test::Unit::TestCase
  include Helper::Cluster

  def test_transaction_with_hash_tag
    rc1 = redis
    rc2 = build_another_client

    rc1.multi

    100.times { |i| rc1.set("{key}#{i}", i) }

    100.times { |i| assert_equal 'QUEUED', rc1.get("{key}#{i}") }
    100.times { |i| assert_equal nil,      rc2.get("{key}#{i}") }

    rc1.exec

    100.times { |i| assert_equal i.to_s, rc1.get("{key}#{i}") }
    100.times { |i| assert_equal i.to_s, rc2.get("{key}#{i}") }
  end

  def test_transaction_without_hash_tag
    rc1 = redis
    rc2 = build_another_client

    rc1.multi

    100.times { |i| rc1.set("key#{i}", i) }

    100.times { |i| assert_equal 'QUEUED', rc1.get("key#{i}") }
    100.times { |i| assert_equal nil,      rc2.get("key#{i}") }

    assert_raise(Redis::Cluster::CommandErrorCollection, 'Command error replied on any node') do
      rc1.exec
    end

    100.times { |i| assert_equal nil, rc1.get("key#{i}") }
    100.times { |i| assert_equal nil, rc2.get("key#{i}") }
  end

  def test_transaction_with_replicas
    rc1 = build_another_client(replica: true)
    rc2 = build_another_client(replica: true)

    rc1.multi

    100.times { |i| rc1.set("{key}#{i}", i) }

    100.times { |i| assert_true ['QUEUED', nil].include?(rc1.get("{key}#{i}")) }
    100.times { |i| assert_equal nil, rc2.get("{key}#{i}") }

    rc1.exec
    sleep 0.1

    100.times { |i| assert_equal i.to_s, rc1.get("{key}#{i}") }
    100.times { |i| assert_equal i.to_s, rc2.get("{key}#{i}") }
  end

  def test_transaction_with_block_and_hash_tag
    rc1 = redis
    rc2 = build_another_client

    rc1.multi do |cli|
      100.times { |i| cli.set("{key}#{i}", i) }
    end

    100.times { |i| assert_equal i.to_s, rc1.get("{key}#{i}") }
    100.times { |i| assert_equal i.to_s, rc2.get("{key}#{i}") }
  end

  def test_transaction_with_block_and_without_hash_tag
    rc1 = redis
    rc2 = build_another_client

    assert_raise(Redis::CommandError, 'MOVED 13252 127.0.0.1:7002') do
      rc1.multi do |cli|
        100.times { |i| cli.set("key#{i}", i) }
      end
    end

    100.times { |i| assert_equal nil, rc1.get("key#{i}") }
    100.times { |i| assert_equal nil, rc2.get("key#{i}") }
  end

  def test_transaction_with_watch
    rc1 = redis
    rc2 = build_another_client

    rc1.set('{key}1', 100)
    rc1.watch('{key}1')

    rc2.set('{key}1', 200)
    val = rc1.get('{key}1').to_i
    val += 1

    rc1.multi
    rc1.set('{key}1', val)
    rc1.set('{key}2', 300)
    rc1.exec

    assert_equal '200', rc1.get('{key}1')
    assert_equal '200', rc2.get('{key}1')

    assert_equal nil, rc1.get('{key}2')
    assert_equal nil, rc2.get('{key}2')
  end
end
