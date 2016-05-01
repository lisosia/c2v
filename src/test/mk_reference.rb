require "sqlite3"
include SQLite3
require "zlib"

db = Database.new("ref.0")
TABLE = "sequence"

schema = <<-SQL
create table  #{TABLE}
(
start int,
end int,
sequence blob,
description_id blob
)
SQL

def mk_seq(len)
  fn = '___tmpfile___738497194'
  s = ( "acgt" * (len / 4 ) + "acgt"[0, len % 4] ).encode('UTF-8')
  Zlib::GzipWriter.open(fn) do |gz|
    gz.write s
  end
  ret = File.binread( File.open(fn) )
  `rm #{fn}`
  return ret
end


def insert_ref( db, pos_s, pos_e, desc_id )
  len = pos_e - pos_s + 1
  sql = <<-SQL
insert into #{TABLE} (start ,end, sequence, description_id) 
VALUES (#{pos_s}, #{pos_e}, ?, #{desc_id} )
SQL
  db.execute sql , Blob.new( mk_seq(len) ) 
end

db.execute(schema)

split = 10
$POS_MIN = 1
$POS_MAX = 110

1.upto 24 do |chr|
  pos = $POS_MIN
  while pos <= $POS_MAX
    pos_end = pos + split - 1
    insert_ref(db, pos, pos_end, chr )
    puts "chr #{chr}, pos #{pos} ~ #{pos_end} done"
    pos += split
  end
end
