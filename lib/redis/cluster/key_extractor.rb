# frozen_string_literal: true

class Redis
  class Cluster
    # Extract key or hashtag from command for Redis Cluster Client
    module KeyExtractor
      module_function

      def extract(command)
        return '' if keyless?(command)

        key = take_first_key(command)
        hash_tag = extract_hash_tag(key)
        hash_tag.empty? ? key : hash_tag
      end

      def keyless?(command)
        return true if command.size < 2

        case command.first.to_s.downcase
        when 'auth', 'client', 'cluster', 'command', 'config', 'debug', 'echo',
             'flushall', 'flushdb', 'info', 'ping', 'script', 'select',
             'shutdown', 'slaveof', 'slowlog', 'swapdb', 'wait'
          true
        when 'memory'
          !command[1].to_s.casecmp('usage').zero?
        else
          false
        end
      end

      def take_first_key(command)
        case command.first.to_s.downcase
        when 'eval', 'evalsha', 'migrate' then command[3]
        when 'object' then command[2]
        when 'memory'
          command[1].to_s.casecmp('usage').zero? ? command[2] : ''
        when 'xread', 'xreadgroup'
          idx = command.map(&:to_s).map(&:downcase).index('streams') + 1
          command[idx]
        else command[1]
        end.to_s
      end

      # @see https://redis.io/topics/cluster-spec#keys-hash-tags Keys hash tags
      def extract_hash_tag(key)
        s = key.index('{')
        e = key.index('}', s.to_i + 1)

        return '' if s.nil? || e.nil?

        key[s + 1..e - 1]
      end
    end
  end
end
