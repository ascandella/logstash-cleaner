#!/usr/bin/env ruby

require 'optparse'
require 'json'
require 'net/http'
require 'uri'

options = {
  :host        => 'localhost',
  :mount_point => '/data',
  :threshold   => 90
}

OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [options]"

  opts.on('-h', '--host HOSTNAME', 'Which ES node to talk to') do |h|
    options[:host] = h
  end

  opts.on('-m', '--mountpoint MOUNTPOINT', 'Where ES keeps its data') do |m|
    options[:mount_point] = m
  end

  opts.on('-t', '--threshold THRESHOLD', Integer,
          'What percent of the disk to use before expiring old indices') do |t|
    options[:threshold] = t
  end
end.parse!

class LogstashCleaner
  def initialize(opts)
    @opts = opts
  end

  def run
    if (du = disk_usage(@opts[:mount_point])) &&
        du > @opts[:threshold]
      puts "Usage of #{du} greater than threshold of #{@opts[:threshold]}"
      purge_oldest(@opts[:host])
    end
  end

private

  def disk_usage(mount_point)
    `df #{mount_point} | tail -n 1 | awk '{ print $4 }'`.chomp.to_i
  end

  def purge_oldest(elasticsearch_host)
    last_index = get_indices(elasticsearch_host).sort.first
    puts "Removing oldest index: #{last_index}"

    request  = Net::HTTP::Delete.new("/#{last_index}")
    http     = Net::HTTP.new(elasticsearch_host, 9200)
    response = http.request(request)

    puts "ES responded: '#{response.body}'"
  end

  def get_indices(elasticsearch_host)
    uri = URI.parse("http://#{elasticsearch_host}:9200/_status")
    response = Net::HTTP.get_response(uri)
    JSON.parse(response.body)['indices'].keys.select do |key|
      key.start_with?('logstash')
    end
  end
end

LogstashCleaner.new(options).run
