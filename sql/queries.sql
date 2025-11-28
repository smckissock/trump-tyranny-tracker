-- SQL Queries for Data Exploration
-- Run these queries in Beekeeper Studio or similar SQL client
-- Database: data/stories.duckdb

-- ============================================================================
-- 1. COUNT OF PUBLICATIONS
-- Shows how many stories each publication has
-- ============================================================================

SELECT 
    source_name,
    COUNT(*) as story_count
FROM story
WHERE source_name IS NOT NULL AND source_name != ''
GROUP BY source_name
ORDER BY story_count DESC;


-- ============================================================================
-- 2. PERCENTAGE OF STORIES WITH BODIES BY PUBLICATION
-- Shows success rate of body extraction for each publication
-- Only shows publications with at least 5 stories
-- ============================================================================

SELECT 
    source_name,
    COUNT(*) as total_stories,
    SUM(CASE WHEN body IS NOT NULL AND body != '' THEN 1 ELSE 0 END) as stories_with_body,
    ROUND(100.0 * SUM(CASE WHEN body IS NOT NULL AND body != '' THEN 1 ELSE 0 END) / COUNT(*), 2) as percentage_with_body
FROM story
WHERE source_name IS NOT NULL AND source_name != ''
GROUP BY source_name
HAVING COUNT(*) >= 5
ORDER BY percentage_with_body DESC, total_stories DESC;


-- ============================================================================
-- 3. TOP 10 LONGEST BODIES
-- Stories with the longest article text
-- ============================================================================

SELECT 
    id,
    source_name,
    LENGTH(body) as body_length,
    SUBSTRING(body, 1, 100) || '...' as body_preview
FROM story
WHERE body IS NOT NULL AND body != ''
ORDER BY body_length DESC
LIMIT 10;


-- ============================================================================
-- 4. TOP 10 SHORTEST BODIES (non-empty)
-- Stories with the shortest article text
-- ============================================================================

SELECT 
    id,
    source_name,
    LENGTH(body) as body_length,
    body as body_preview
FROM story
WHERE body IS NOT NULL AND body != ''
ORDER BY body_length ASC
LIMIT 10;


-- ============================================================================
-- 5. BODY LENGTH STATISTICS
-- Summary statistics for article body lengths
-- ============================================================================

SELECT 
    COUNT(*) as total_with_body,
    AVG(LENGTH(body)) as avg_length,
    MIN(LENGTH(body)) as min_length,
    MAX(LENGTH(body)) as max_length,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH(body)) as median_length
FROM story
WHERE body IS NOT NULL AND body != '';


-- ============================================================================
-- 6. RANK OF AUTHORS BY NUMBER OF STORIES
-- Shows which authors have written the most stories
-- ============================================================================

SELECT 
    authors,
    COUNT(*) as story_count
FROM story
WHERE authors IS NOT NULL AND authors != ''
GROUP BY authors
ORDER BY story_count DESC;


-- ============================================================================
-- 7. AUTHOR STATISTICS
-- Summary statistics about authors
-- ============================================================================

SELECT 
    COUNT(DISTINCT authors) as unique_authors,
    COUNT(*) as stories_with_authors,
    AVG(story_count) as avg_stories_per_author
FROM (
    SELECT authors, COUNT(*) as story_count
    FROM story
    WHERE authors IS NOT NULL AND authors != ''
    GROUP BY authors
);


-- ============================================================================
-- 8. GROUP AND COUNT BY ITEM_SECTION
-- Shows how many stories are in each section
-- ============================================================================

SELECT 
    item_section,
    COUNT(*) as story_count
FROM story
WHERE item_section IS NOT NULL AND item_section != ''
GROUP BY item_section
ORDER BY story_count DESC;


-- ============================================================================
-- 9. OVERALL STATISTICS
-- Complete overview of the database
-- ============================================================================

SELECT 
    COUNT(*) as total_stories,
    COUNT(DISTINCT post_name) as unique_posts,
    COUNT(DISTINCT source_name) as unique_sources,
    SUM(CASE WHEN source_url IS NOT NULL AND source_url != '' THEN 1 ELSE 0 END) as stories_with_url,
    SUM(CASE WHEN title IS NOT NULL AND title != '' THEN 1 ELSE 0 END) as stories_with_title,
    SUM(CASE WHEN body IS NOT NULL AND body != '' THEN 1 ELSE 0 END) as stories_with_body,
    SUM(CASE WHEN authors IS NOT NULL AND authors != '' THEN 1 ELSE 0 END) as stories_with_authors,
    SUM(CASE WHEN image IS NOT NULL AND image != '' THEN 1 ELSE 0 END) as stories_with_image,
    SUM(CASE WHEN errors IS NOT NULL AND errors != '' THEN 1 ELSE 0 END) as stories_with_errors
FROM story;

