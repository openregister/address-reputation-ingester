require 'abpprocess/version'
require 'abpprocess/abpprocessor'
require 'optparse'

module Abpprocess
  class Main

    def self.run
      (optsparse,options) = processOptions
      optsparse.parse!

      if ARGV.length == 0
        puts optsparse
        exit 0
      end

      options[:name] = ARGV.shift

      if ARGV.length == 0
        processor = AbpProcessor.createFromDir('.', options)
      else
        processor = AbpProcessor.createFromFiles(ARGV, options)
      end

      processor.process
      processor.importToPostgres unless options[:csvs_only]
    end

    private

    DEFAULT_OPTIONS = {:remove_csv => false, :postgres_only => false, :csvs_only => false}

    def self.processOptions
      options = DEFAULT_OPTIONS.clone
      optsparser = OptionParser.new do |opts|
        opts.banner = 'Usage: abpprocess [options] [product_and_ver] [files]'

        opts.on('-r', 'Remove unzipped CSV files') do
          options[:remove_csv] = true
        end

        opts.on('-c', 'Generate type CSVs only') do
          options[:csvs_only] = true
        end

        opts.on('-p', 'Import only - assumes existing types CSVs') do
          options[:postgres_only] = true
        end

      end

      return optsparser,options
    end

  end
end
