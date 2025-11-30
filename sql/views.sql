CREATE OR REPLACE VIEW story_view AS  
SELECT 
  s.id,
  s.post_name,
  s.item_section_type,
  s.item_section,
  s.item_what_happened,
  s.item_why_it_matters,
  s.source_name,
  s.source_url,
  s.title,
  s.authors,
  s.image,
  s.post_date,
  COALESCE(s.publish_date, strptime(s.post_date, '%b %d, %Y')::DATE) AS publish_date
FROM story s;


-- ============================================================================
-- ENTITY VIEWS FOR SEARCH EXPORT
-- ============================================================================

-- Top 1000 entities ranked by total weighted score
CREATE OR REPLACE VIEW entity_score_view AS
SELECT 
  e.id,
  e.name,
  e.type,
  e.count as story_count,
  ROUND(SUM(se.score), 1) as total_score
FROM entity e
JOIN story_entity se ON e.id = se.entity_id
WHERE e.active = true
GROUP BY e.id, e.name, e.type, e.count
ORDER BY total_score DESC
LIMIT 1000;


-- Story IDs for each top entity (for search filtering)
CREATE OR REPLACE VIEW entity_stories_view AS
SELECT 
  esv.name,
  esv.type,
  esv.story_count,
  esv.total_score,
  LIST(se.story_id ORDER BY se.score DESC) as story_ids
FROM entity_score_view esv
JOIN story_entity se ON esv.id = se.entity_id
GROUP BY esv.name, esv.type, esv.story_count, esv.total_score
ORDER BY esv.total_score DESC;

