players = load 'baseball' as (name:chararray, team:chararray,
            position:bag{t:(p:chararray)}, bat:map[]);
pos     = foreach players generate name, flatten(position) as position;
bypos   = group pos by position;