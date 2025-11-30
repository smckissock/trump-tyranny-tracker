CREATE OR REPLACE VIEW story_view AS  
SELECT 
  s.id,
  s.post_name,
  s.item_section_type,
  s.item_what_happened,
  s.item_why_it_matters,
  s.source_name,
  s.title,
  s.authors,
  s.image,
  s.post_date,
  COALESCE(s.publish_date, strptime(s.post_date, '%b %d, %Y')::DATE) AS publish_date
FROM story s;

