pigInputFile = LOAD 'hdfs:///user/phani/piginputfiles/episodeVI_dialouges.txt' USING PigStorage('\t') AS (character:chararray,dialogue:chararray);
groupByCharacter = GROUP pigInputFile BY character;
dialogueCount = FOREACH groupByCharacter GENERATE $0 as character, COUNT($1) as noOfLines;
orderedDialogueCount = ORDER dialogueCount BY noOfLines DESC;
STORE orderedDialogueCount INTO 'hdfs:///user/phani/EpisodeVIDialogueCount/dialogueCount' USING PigStorage('\t');
STORE orderedDialogueCount INTO '/EpisodeVIDialogueCount/dialogueCount' USING PigStorage('\t');
