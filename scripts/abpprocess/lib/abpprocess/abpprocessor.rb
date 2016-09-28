require 'abpprocess/abppostgresimporter'
require 'zip'
require 'zlib'
require 'logger'
require 'pg'

class AbpProcessor

  include AbpPostgresImporter

  def initialize(files, options = {})
    @logger = Logger.new(STDOUT)
    @zip_files = files
    @options = options
    @filehash = {}

    @DIR_STRUCTURE = {
        :CSV_INT => '../unpacked',
        :TYPES => '../types',
    }

    @HEADERS = {
        :'10' => 'Record_10_HEADER_Header.csv',
        :'11' => 'Record_11_STREET_Header.csv',
        :'15' => 'Record_15_STREETDESCRIPTOR_Header.csv',
        :'21' => 'Record_21_BLPU_Header.csv',
        :'23' => 'Record_23_XREF_Header.csv',
        :'24' => 'Record_24_LPI_Header.csv',
        :'28' => 'Record_28_DELIVERYPOINTADDRESS_Header.csv',
        :'29' => 'Record_29_METADATA_Header.csv',
        :'30' => 'Record_30_SUCCESSOR_Header.csv',
        :'31' => 'Record_31_ORGANISATION_Header.csv',
        :'32' => 'Record_32_CLASSIFICATION_Header.csv',
        :'99' => 'Record_99_TRAILER_Header.csv',
    }

    AbpProcessor.createDirs(@DIR_STRUCTURE.values)
  end

  def self.createDirs(dirs)
    dirs.each do |dir|
      FileUtils.mkpath(dir) unless Dir.exist?(dir)
    end
  end

  def self.createFromFiles(files, options = {})
    AbpProcessor.new(files, options)
  end

  def self.createFromDir(dir, options = {})
    files = Array.new
    Dir.entries('.').each do |entry|
      next unless entry.end_with?('zip')
      files << entry
    end

    AbpProcessor.new(files, options)
  end

  def process
    start = Time::now
    @zip_files.each do |file|
      extracted = unzipZip(file)
      processCSV(extracted) unless @options[:postgres_only]
      removeCSVFile(extracted)
    end

    stop = Time::now
    @logger.info("Processing time: #{stop-start} seconds")

    closeTypeFiles
  end

  def importToPostgres
    doImportToPostgres @DIR_STRUCTURE[:TYPES]
  end

  private

  def unzipZip(file)
    extract_name = nil
    Zip::File.open(file) do |z|
      z.each do |entry|
        @logger.info("Extracting zip file <#{file}>")
        extract_name=@DIR_STRUCTURE[:CSV_INT] + '/' + entry.name

        File.delete(extract_name) if (File.exists?(extract_name))
        z.extract(entry, extract_name)
      end
    end
    extract_name
  end

  def processCSV(infile)
    @logger.info("Processing csv <#{infile}>")
    File.open(infile) do |f|
      until f.eof
        line = f.readline
        idx = line.index(',')
        next if idx==-1

        f1 = line[0, idx]
        outfile = @filehash[f1]
        unless outfile
          new_file=@DIR_STRUCTURE[:TYPES] + "/t#{f1}.csv.gz"
          @logger.info("Creating new output file <#{new_file}>")
          outfile = Zlib::GzipWriter.new(File.new(new_file, 'w'))

          hdr_file=File.dirname(__FILE__)+"/../../resources/headers/#{@HEADERS[f1.to_s.to_sym]}"
          outfile.write(File.new(hdr_file, 'r').readline)
          @filehash[f1]=outfile
        end

        outfile.write line

      end
      f.close
    end
  end

  def closeTypeFiles
    @filehash.each_value do |file|
      file.close
    end
  end

  def removeCSVFile(file)
    File.delete(file) if @options[':remove_csv']
  end

end
