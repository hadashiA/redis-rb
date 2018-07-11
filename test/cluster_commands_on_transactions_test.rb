# frozen_string_literal: true

require_relative 'helper'

# ruby -w -Itest test/cluster_commands_on_transactions_test.rb
# @see https://redis.io/commands#transactions
class TestClusterCommandsOnTransactions < Test::Unit::TestCase
  include Helper::Cluster

  def test_discard
    assert_raise(Redis::Cluster::CommandErrorCollection, 'Command error replied on any node') do
      redis.discard
    end

    redis.multi
    assert_equal 'OK', redis.discard
  end

  def test_exec
    assert_raise(Redis::Cluster::CommandErrorCollection, 'Command error replied on any node') do
      redis.exec
    end

    redis.multi
    assert_equal [], redis.exec
  end

  def test_multi
    assert_equal 'OK', redis.multi
    redis.discard
  end

  def test_unwatch
    assert_equal 'OK', redis.unwatch
  end

  def test_watch
    assert_raise(Redis::CommandError, "CROSSSLOT Keys in request don't hash to the same slot") do
      redis.watch('key1', 'key2')
    end

    assert_equal 'OK', redis.watch('{key}1', '{key}2')
    redis.unwatch
  end
end
