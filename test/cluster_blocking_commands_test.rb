# frozen_string_literal: true

require_relative 'helper'
require_relative 'lint/blocking_commands'

# ruby -w -Itest test/cluster_blocking_commands_test.rb
class TestClusterBlockingCommands < Test::Unit::TestCase
  include Helper::Cluster
  include Lint::BlockingCommands

  def mock(options = {}, &blk)
    commands = {
      blpop: lambda do |*args|
        sleep options[:delay] if options.key?(:delay)
        to_protocol([args.first, args.last])
      end,
      brpop: lambda do |*args|
        sleep options[:delay] if options.key?(:delay)
        to_protocol([args.first, args.last])
      end,
      brpoplpush: lambda do |*args|
        sleep options[:delay] if options.key?(:delay)
        to_protocol(args.last)
      end
    }

    redis_cluster_mock(commands, &blk)
  end
end
