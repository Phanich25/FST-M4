pigInputFile = LOAD 'hdfs:///user/phani/piginputfiles/episodeIV_dialouges.txt' USING PigStorage('\t') AS (character:chararray,dialogue:chararray);
groupByCharacter = GROUP pigInputFile BY character;
dialogueCount = FOREACH groupByCharacter GENERATE $0 as character, COUNT($1) as noOfLines;
orderedDialogueCount = ORDER dialogueCount BY noOfLines DESC;
STORE orderedDialogueCount INTO 'hdfs:///user/phani/EpisodeIVDialogueCount/dialogueCount' USING PigStorage('\t');
STORE orderedDialogueCount INTO '/EpisodeIVDialogueCount/dialogueCount' USING PigStorage('\t');
