OutFile="checkSex"
exit if ARGV.length != 1
File.open(OutFile, "a") do |f|
  f.write "#{ARGV[0]}\t100\t1000\n"
end
