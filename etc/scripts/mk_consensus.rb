# -*- coding: utf-8 -*-

BASE = ["A","C","G","T"]
GET_SAMPLE_PROB = 0.01 # 有効なcallの確率
ALT_PROB = 0.0007 # 変異率 0.07%
$REF_RAND = Random.new(1234) #referenecを作るためのseed 変えてはいけない


def mk_qual
  return rand(60)
end
def mk_info
  return "DP=#{rand(10)+1};XXX=100"
end

def mk_ref
  return BASE[ $REF_RAND.rand(4) ]
end

def mk_ref_and_alts
  ref_dx = $REF_RAND.rand(4)
  if ( rand() < 0.5 ) then # both change
    alt_dx1 = ( ref_dx + rand(3) + 4 ) % 4
    begin
      alt_dx2 = ( ref_dx + rand(3) + 4 ) % 4
    end while alt_dx1 == alt_dx2
    return "#{BASE[ref_dx]}\t#{BASE[alt_dx1]},#{BASE[alt_dx2]}"
  else
    alt_dx = ( ref_dx + rand(3) + 4 ) % 4
    return "#{BASE[ref_dx]}\t#{BASE[alt_dx]}"
  end
end

def return_line_norm(chr,pos)
  return  "chr#{chr}\t#{pos}\t.\t#{mk_ref()}\t.\t#{mk_qual()}\t.\t#{mk_info()}\tPL\tBAM_INFO"
end
def return_line_alt(chr,pos)
  return  "chr#{chr}\t#{pos}\t.\t#{mk_ref_and_alts()}\t#{mk_qual()}\t.\t#{mk_info()}\tPL\tBAM_INFO"
end

$CHR=12
last = ARGV[0].to_i
for pos in 1...last
  
  next if rand() > GET_SAMPLE_PROB
  if rand() > ALT_PROB
    puts return_line_norm( $CHR,pos )
  else
    puts return_line_alt( $CHR,pos )
  end
  
end
