require 'sqskiq/signal_handler'

module Sqskiq
  class Manager
    include Celluloid
    include Sqskiq::SignalHandler

    @empty_queue = false

    def initialize(worker_class)

      config = valid_config_from(worker_class)

      @processor = Processor.pool(:size => config[:num_workers], :args => worker_class)

      @fetcher = Fetcher.pool(:size => config[:num_fetchers], args: [{ queue_name: config[:queue_name], configuration: Sqskiq.configuration, manager: self }])
      @deleter = Deleter.pool(:size => config[:num_deleters], args: [{ queue_name: config[:queue_name], configuration: Sqskiq.configuration }])
      @batcher = BatchProcessor.pool(size: config[:num_batches], args: [{ manager: self, processor: @processor }])

      @empty_queue_throttle = config[:empty_queue_throttle]

      subscribe_for_shutdown

    end

    def bootstrap
      new_fetch(@fetcher.size)
    end

    def fetch_done(messages)
      @empty_queue = messages.empty?
      @batcher.async.process(messages) unless @shutting_down
    end

    def batch_done(messages)
      @deleter.async.delete(messages)
      new_fetch(1)
    end

    def new_fetch(num)
      after(throttle) do
        num.times { @fetcher.async.fetch unless @shutting_down }
      end
    end

    def running?
       !(@shutting_down && @deleter.busy_size == 0 && @batcher.busy_size == 0)
    end

    def throttle
      @empty_queue ? @empty_queue_throttle : 0
    end

    def valid_config_from(worker_class)
      worker_config = worker_class.sqskiq_options_hash
      num_workers = (worker_config[:processors].nil? || worker_config[:processors].to_i < 2)? 20 : worker_config[:processors]
      # messy code due to celluloid pool constraint of 2 as min pool size: see spec for better understanding
      num_fetchers = num_workers / 10
      num_fetchers = num_fetchers + 1 if num_workers % 10 > 0
      num_fetchers = 2 if num_fetchers < 2
      num_deleters = num_batches = num_fetchers

      {
        num_workers: num_workers,
        num_fetchers: num_fetchers,
        num_batches: num_batches,
        num_deleters: num_deleters,
        queue_name: worker_config[:queue_name],
        empty_queue_throttle: worker_config[:empty_queue_throttle] || 0
      }

    end

    def prepare_for_shutdown
      self.publish('SIGTERM')
      @batcher.publish('SIGTERM')
      @processor.publish('SIGTERM')
    end


  end
end