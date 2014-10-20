require 'sqskiq/worker'

module Sqskiq

  def self.initialize!
    require 'celluloid'
    require 'celluloid/autostart'

    require "sqskiq/manager"
    require 'sqskiq/fetch'
    require 'sqskiq/process'
    require 'sqskiq/delete'
    require 'sqskiq/batch_process'
  end

  # Configures and starts actor system
  def self.bootstrap
    initialize!

    managers = []

    worker_classes.each do |worker_class|
      managers << Manager.new(worker_class)
    end

    run!(managers)
  rescue Interrupt
    exit 0
  end

  # Subscribes actors to receive system signals
  # Each actor when receives a signal should execute
  # appropriate code to exit cleanly
  def self.run!(managers)
    self_read, self_write = IO.pipe

    ['SIGTERM', 'TERM', 'SIGINT'].each do |sig|
      begin
        trap sig do
          self_write.puts(sig)
        end
      rescue ArgumentError
        puts "Signal #{sig} not supported"
      end
    end


    begin

      managers.each(&:bootstrap)

      while readable_io = IO.select([self_read])

        signal = readable_io.first[0].gets.strip

        managers.each(&:prepare_for_shutdown)

        while managers.all?(&:running?)
          sleep 2
        end

        managers.each(&:terminate)

        break
      end
    end
  end


  def self.configure
    yield self
  end

  def self.configuration
    @configuration
  end

  def self.configuration=(value)
    @configuration = value
  end

  def self.worker_classes

    worker_file_names.inject([]) do |workers, file_name|

      worker_class = file_name.gsub('.rb','').camelize.constantize

      if worker_class.ancestors.include?(Sqskiq::Worker)
        workers << worker_class
      end
      workers
    end
  end

  def self.worker_file_names
    Dir.entries(Rails.root.join('app', 'workers')).select { |file_name| file_name.end_with?('worker.rb') }.reverse
  end

end
