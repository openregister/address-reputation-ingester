require 'zlib'

module AbpPostgresImporter


  def doImportToPostgres(dir)

    dbname = @options[:name]

    conn = PG.connect(dbname: 'postgres')
    createDatabase(conn, dbname)

    # reconnect to the newly created db
    conn = PG.connect(dbname: dbname)

    importSchema(conn)

    importFiles(conn, dir)
  end

  private

  def addIndex(conn, table)
    indexes[table.to_sym].each do |idx|
      start = Time::now
      @logger.info("Creating index idx_#{table}_#{idx} ON #{table} (#{idx})")
      conn.exec("CREATE INDEX idx_#{table}_#{idx} ON #{table} (#{idx})")
      stop = Time::now
      @logger.info("Index created in #{stop-start}seconds")
    end
  end

  def indexes
    {
        :abp_street => ['usrn'],
        :abp_street_descriptor => ['usrn'],
        :abp_blpu => ['uprn', 'postcode_locator'],
        :abp_lpi => ['uprn'],
        :abp_delivery_point => ['uprn'],
    }
  end

  def fileTableMap
    {
        :'t11.csv.gz' => 'abp_street',
        :'t15.csv.gz' => 'abp_street_descriptor',
        :'t21.csv.gz' => 'abp_blpu',
        :'t24.csv.gz' => 'abp_lpi',
        :'t28.csv.gz' => 'abp_delivery_point',
    }
  end

  def copyData(conn, file, table)
    @logger.info("Processing <#{file}>")
    buf = ''
    count = 0
    start = Time::now

    conn.exec("COPY #{table} FROM STDIN WITH CSV HEADER")

    Zlib::GzipReader.open(file) do |f|
      until f.eof
        line = f.readline
        count+=1

        until conn.put_copy_data(line)
          sleep 0.1
        end

      end

      conn.put_copy_end
      stop = Time::now

      while res = conn.get_result
        @logger.info("Result of COPY is: %s" % [res.res_status(res.result_status)])
        @logger.info("Loaded #{count} records in #{stop-start} seconds")
      end
    end
  end

  def importFiles(conn, dir)
    ftm = fileTableMap
    Dir.entries(dir).each do |f|
      next unless f.end_with?('csv.gz')

      table = ftm[f.to_sym]

      next unless table

      copyData(conn, "#{dir}/#{f}", table)

      addIndex(conn, table)
    end
  end

  def importSchema(conn)
    File.open(File.dirname(__FILE__)+'/../../resources/pg-scripts/PostgreSQL_AddressBase_Premium_CreateTable.sql') do |f|
      str = f.read
      conn.exec(str)
    end
  end

  def createDatabase(conn, dbname)
    conn.exec("DROP DATABASE IF EXISTS #{dbname}")
    conn.exec("CREATE DATABASE #{dbname}")
  end

end
