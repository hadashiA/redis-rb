# frozen_string_literal: true

require 'set'

class Redis
  class Cluster
    # Keep slot and node key map for Redis Cluster Client
    class Slot
      def initialize(available_slots, node_flags = {}, with_replica = false)
        @map = build_slot_node_key_map(available_slots, node_flags, with_replica)
      end

      def exists?(slot)
        @map.key?(slot)
      end

      def not_exists?(slot)
        !exists?(slot)
      end

      def find_node_key(slot)
        @map[slot].to_a.sample
      end

      def put(slot, node_key)
        @map[slot] = Set.new unless @map.key?(slot)
        @map[slot].add(node_key)
        nil
      end

      private

      def build_slot_node_key_map(available_slots, node_flags, with_replica)
        available_slots.each_with_object({}) do |(node_key, slots), acc|
          next if !with_replica && node_flags[node_key] == 'slave'

          slots.each do |slot|
            acc[slot] = Set.new unless acc.key?(slot)
            acc[slot].add(node_key)
          end
        end
      end
    end
  end
end
