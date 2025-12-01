-- RUn the folloing after every import - especially the first!!

UPDATE story 
SET item_section_type = '🛡️ In Power Consolidation News' 
WHERE item_section_type ='🛡️In Power Consolidation News'

UPDATE story SET authors = '' WHERE authors IS NULL

-- Keep this guy, just remove junk on end:  Steve Beynon, Read Full Bio
UPDATE story 
SET authors = REPLACE(authors, ', Read Full Bio', '')
WHERE authors LIKE '%, Read Full Bio';  

UPDATE story 
SET authors = REPLACE(authors, ', Senior Correspondent', '')
WHERE authors LIKE '%, Senior Correspondent';

UPDATE story 
SET authors = REPLACE(authors, ', White House Correspondent', '')
WHERE authors LIKE '%, White House Correspondent';

UPDATE story 
SET authors = REPLACE(authors, ', Senior Reporter', '')
WHERE authors LIKE '%, Senior Reporter';


-- INSERT INTO bad_author VALUES ('Written By')
-- INSERT INTO bad_author VALUES ('Nov, At Am');
-- INSERT INTO bad_author VALUES ('Arden,%');
-- INSERT INTO bad_author VALUES ('Abc News');
-- INSERT INTO bad_author VALUES ('%.Wp-Bloc%');
-- INSERT INTO bad_author VALUES ('%About The Author%');
-- INSERT INTO bad_author VALUES ('_______________________________________%');  -- matches anything longer than 40 chars
-- INSERT INTO bad_author VALUES ('Oct, At Am, Nov');
-- INSERT INTO bad_author VALUES ('Written By%');
-- INSERT INTO bad_author VALUES ('%, At Am, %');
-- INSERT INTO bad_author VALUES ('%, At Pm, %');
-- INSERT INTO bad_author VALUES ('Story The Associated Press');
-- INSERT INTO bad_author VALUES ('The Moscow Times, Oct.');
-- INSERT INTO bad_author VALUES ('Jack');
-- INSERT INTO bad_author VALUES ('The Associated');
-- INSERT INTO bad_author VALUES ('The Moscow Times%');
-- INSERT INTO bad_author VALUES ('%Pm Edt%');
-- INSERT INTO bad_author VALUES ('Err');


--SELECT authors, COUNT(*) FROM story_view GROUP BY ALL ORDER BY COUNT(*) 