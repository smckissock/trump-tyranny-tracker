"""Scraper to extract structured data from individual Substack post pages."""

import re
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Set up logging
logger = logging.getLogger(__name__)


class PostScraper:
    """Scraper for individual Trump Tyranny Tracker post pages."""
    
    def __init__(self):
        pass
    
    def fetch_post_page(self, url: str, max_retries: int = 3) -> str:
        """Fetch a post page HTML with retry logic."""
        logger.info(f"Fetching post page: {url}")
        
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    
                    # Use domcontentloaded (faster) with increasing timeout on retries
                    timeout = 30000 + (attempt - 1) * 15000  # 30s, 45s, 60s
                    page.goto(url, wait_until='domcontentloaded', timeout=timeout)
                    
                    # Wait for article content to load
                    try:
                        page.wait_for_selector('article, .post-content, .body-markup, h3', timeout=15000)
                    except:
                        logger.warning(f"Timeout waiting for content selector on {url}")
                    
                    # Additional wait for dynamic content
                    page.wait_for_timeout(2000)
                    
                    html = page.content()
                    browser.close()
                    
                return html
                
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(f"Attempt {attempt}/{max_retries} failed for {url}: {e}. Retrying...")
                else:
                    logger.error(f"All {max_retries} attempts failed for {url}")
                    raise last_error
    
    def parse_post(self, html: str, post_url: str) -> Dict:
        """Parse a post page and extract structured data."""
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract post name (title)
        post_name = self._extract_post_name(soup)
        
        # Extract author and date
        author, date = self._extract_author_and_date(soup)
        
        # Extract all sections and items
        items = self._extract_items(soup)
        
        return {
            'post_name': post_name,
            'post_author': author,
            'post_date': date,
            'post_url': post_url,
            'items': items
        }
    
    def _extract_post_name(self, soup: BeautifulSoup) -> str:
        """Extract the post title."""
        # Try multiple selectors for the title
        selectors = [
            'h1.post-title',
            'h1[class*="title"]',
            'h1',
            '.post-title',
            'article h1',
        ]
        
        for selector in selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                if title:
                    return title
        
        # Fallback: look for any h1
        h1 = soup.find('h1')
        if h1:
            return h1.get_text(strip=True)
        
        return ""
    
    def _extract_author_and_date(self, soup: BeautifulSoup) -> Tuple[str, str]:
        """Extract author and date from the post."""
        author = ""
        date = ""
        
        # Look for author in meta tag (most reliable for Substack)
        author_meta = soup.select_one('meta[name="author"]')
        if author_meta and author_meta.get('content'):
            author = author_meta.get('content')
        
        # Fallback: Substack profile links
        if not author:
            author_link = soup.select_one('a[href*="substack.com/@"]')
            if author_link:
                author = author_link.get_text(strip=True)
        
        # Fallback to common patterns
        if not author:
            author_selectors = [
                '.author-name',
                '[class*="author"]',
                '.byline',
                '.post-meta',
                '.profile-hover-card-target a',
            ]
            
            for selector in author_selectors:
                author_elem = soup.select_one(selector)
                if author_elem:
                    author_text = author_elem.get_text(strip=True)
                    if author_text and not author_text.startswith('Nov') and not author_text.startswith('2025'):
                        author = author_text
                        break
        
        # Look for date - often near author or in meta
        date_patterns = [
            r'(Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct)\s+\d{1,2},\s+\d{4}',
            r'\d{1,2}\s+(Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct)\s+\d{4}',
        ]
        
        # Look in various places
        text_content = soup.get_text()
        for pattern in date_patterns:
            match = re.search(pattern, text_content)
            if match:
                date = match.group(0)
                break
        
        # Also try to find date in specific elements
        if not date:
            date_selectors = [
                '.post-date',
                '[class*="date"]',
                'time',
            ]
            for selector in date_selectors:
                date_elem = soup.select_one(selector)
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    if re.search(r'\d{4}', date_text):
                        date = date_text
                        break
        
        return author, date
    
    def _extract_items(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract all items from all sections in the post."""
        items = []
        
        # Get the main content area
        content = soup.find('article') or soup.find('main') or soup.find('div', class_=re.compile('post|content'))
        
        if not content:
            # Try to find content in body
            content = soup.find('body')
        
        if not content:
            return items
        
        # Find all section headers - they are in <strong> tags with emoji
        section_pattern = re.compile(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News')
        
        # Look for section headers in strong tags first (most common)
        section_headers = []
        strong_tags = content.find_all('strong')
        for tag in strong_tags:
            text = tag.get_text(strip=True)
            if section_pattern.search(text):
                section_headers.append(tag)
        
        # Also check h2, h3, h4 as fallback
        if not section_headers:
            for tag_name in ['h2', 'h3', 'h4']:
                headers = content.find_all(tag_name, string=section_pattern)
                section_headers.extend(headers)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_headers = []
        for header in section_headers:
            text = header.get_text(strip=True)
            if text not in seen:
                seen.add(text)
                unique_headers.append(header)
        
        if unique_headers:
            # Extract items from each section
            for i, header in enumerate(unique_headers):
                section_name = header.get_text(strip=True)
                next_header = unique_headers[i + 1] if i + 1 < len(unique_headers) else None
                
                section_items = self._extract_items_in_section(header, next_header, section_name)
                items.extend(section_items)
        else:
            # Fallback: try to extract items without clear section structure
            items = self._extract_items_without_sections(content)
        
        return items
    
    def _extract_items_by_structure(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract items by looking for the common structure pattern."""
        items = []
        
        # Get the main content area
        content = soup.find('article') or soup.find('main') or soup.find('div', class_=re.compile('post|content'))
        
        if not content:
            return items
        
        # Find all section headers
        section_headers = content.find_all(['h2', 'h3'], string=re.compile(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News'))
        
        current_section = ""
        
        # Process each section
        for i, section_header in enumerate(section_headers):
            current_section = section_header.get_text(strip=True)
            
            # Get content between this section and the next
            next_header = section_headers[i + 1] if i + 1 < len(section_headers) else None
            
            # Find all elements in this section
            section_items = self._extract_items_in_section(section_header, next_header, current_section)
            items.extend(section_items)
        
        # If no section headers found, try to find items anyway
        if not items:
            items = self._extract_items_without_sections(content)
        
        return items
    
    def _extract_items_in_section(self, section_start, section_end, section_name: str) -> List[Dict]:
        """Extract items from a specific section."""
        items = []
        
        # If section_start is a <strong> tag, we need to look at its parent's siblings
        # or find the next elements in the document
        if section_start.name == 'strong':
            # Get the parent element (usually a <p> or <div>)
            parent = section_start.parent
            if parent:
                # Start from the parent's next sibling
                current = parent.next_sibling
            else:
                # Fallback: start from section_start's next sibling
                current = section_start.find_next_sibling()
        else:
            current = section_start.next_sibling if hasattr(section_start, 'next_sibling') else None
        
        # Collect all elements in this section
        section_elements = []
        while current:
            # Stop if we hit the next section
            if current == section_end:
                break
            
            # Check if this is another section header
            if hasattr(current, 'get_text'):
                text = current.get_text(strip=True)
                if re.search(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News', text):
                    break
            
            section_elements.append(current)
            
            # Move to next sibling
            if hasattr(current, 'next_sibling'):
                current = current.next_sibling
            else:
                break
        
        # Now parse items from the collected elements
        i = 0
        while i < len(section_elements):
            elem = section_elements[i]
            
            # Look for item titles - they're in <h3 class="header-anchor-post">
            is_item_title = False
            title_text = ""
            
            if hasattr(elem, 'name') and hasattr(elem, 'get_text'):
                text = elem.get_text(strip=True)
                
                # Skip section headers
                if re.search(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News', text):
                    i += 1
                    continue
                
                # Item titles are in <h3 class="header-anchor-post"> tags
                if elem.name == 'h3':
                    # Check if it has the header-anchor-post class
                    classes = elem.get('class', [])
                    if 'header-anchor-post' in classes or not classes:
                        # Pattern to match section headers (emoji + "In ... News")
                        section_pattern = r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News'
                        
                        item_section_type = section_name  # Default to current section
                        item_section = ""  # The item title
                        
                        strong_tag = elem.find('strong')
                        if strong_tag:
                            strong_text = strong_tag.get_text(strip=True)
                            
                            # Check if this is a section header (newer format: strong contains emoji section)
                            if re.search(section_pattern, strong_text):
                                # This is a section header, skip it
                                i += 1
                                continue
                            else:
                                # This is an item title (older format: strong contains title)
                                item_section = strong_text
                                # item_section_type stays as the current section
                        else:
                            # No strong tag
                            # Check if this is a section header (older format: h3 text contains emoji section)
                            if re.search(section_pattern, text):
                                # This is a section header, skip it
                                i += 1
                                continue
                            else:
                                # This is an item title (newer format: h3 text is the title)
                                item_section = text
                        
                        title_text = item_section
                        # Item titles are usually longer than 20 chars
                        is_item_title = len(title_text) > 20
            
            if is_item_title and title_text:
                # Ensure variables are defined
                if 'item_section_type' not in locals():
                    item_section_type = section_name
                if 'item_section' not in locals():
                    item_section = ''
                
                item = {
                    'item_section_type': item_section_type,
                    'item_section': item_section,
                    'item_what_happened': '',
                    'item_why_it_matters': '',
                    'source_name': '',
                    'source_url': ''
                }
                
                # Extract details from following elements
                item = self._extract_item_details_from_list(section_elements, i, item, section_end)
                
                # Only add item if we have content (what_happened or why_it_matters)
                if item.get('item_what_happened') or item.get('item_why_it_matters'):
                    items.append(item)
            
            i += 1
        
        return items
    
    def _extract_item_details_from_list(self, elements: List, start_idx: int, item: Dict, section_end) -> Dict:
        """Extract What Happened, Why It Matters, and Source from a list of elements."""
        found_what_happened = False
        found_why_matters = False
        
        i = start_idx + 1
        while i < len(elements):
            elem = elements[i]
            
            if not hasattr(elem, 'get_text'):
                i += 1
                continue
            
            text = elem.get_text(strip=True)
            
            # Check if we've hit the next item (new h3 with header-anchor-post class)
            if hasattr(elem, 'name'):
                if elem.name == 'h3':
                    classes = elem.get('class', [])
                    if 'header-anchor-post' in classes or not classes:
                        if len(text) > 20 and not re.search(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨]', text):
                            break  # Next item found
            
            # Look for "What Happened:" in <p><strong>What Happened:</strong><span>content</span></p>
            if ('What Happened' in text or 'What Happened:' in text) and not found_what_happened:
                if elem.name == 'p':
                    # Look for span inside the paragraph
                    span = elem.find('span')
                    if span:
                        what_happened = span.get_text(strip=True)
                        if what_happened:
                            item['item_what_happened'] = what_happened
                            found_what_happened = True
                    else:
                        # Fallback: extract text after "What Happened:"
                        what_happened = re.sub(r'^.*?What Happened:?\s*', '', text, flags=re.IGNORECASE)
                        if what_happened and len(what_happened) > 10:
                            item['item_what_happened'] = what_happened
                            found_what_happened = True
                else:
                    # Fallback: extract text after "What Happened:"
                    what_happened = re.sub(r'^.*?What Happened:?\s*', '', text, flags=re.IGNORECASE)
                    if what_happened and len(what_happened) > 10:
                        item['item_what_happened'] = what_happened
                        found_what_happened = True
            
            # Look for "Why It Matters:" in <p><strong>Why It Matters:</strong><span>content</span></p>
            elif ('Why It Matters' in text or 'Why It Matters:' in text) and not found_why_matters:
                if elem.name == 'p':
                    # Look for span inside the paragraph
                    span = elem.find('span')
                    if span:
                        why_matters = span.get_text(strip=True)
                        if why_matters:
                            item['item_why_it_matters'] = why_matters
                            found_why_matters = True
                    else:
                        # Fallback: extract text after "Why It Matters:"
                        why_matters = re.sub(r'^.*?Why It Matters:?\s*', '', text, flags=re.IGNORECASE)
                        if why_matters and len(why_matters) > 10:
                            item['item_why_it_matters'] = why_matters
                            found_why_matters = True
                else:
                    # Fallback: extract text after "Why It Matters:"
                    why_matters = re.sub(r'^.*?Why It Matters:?\s*', '', text, flags=re.IGNORECASE)
                    if why_matters and len(why_matters) > 10:
                        item['item_why_it_matters'] = why_matters
                        found_why_matters = True
            
            # Look for source in <pre><code> blocks (newer format)
            elif hasattr(elem, 'name') and elem.name == 'pre':
                code = elem.find('code')
                if code:
                    # Look for link inside code
                    link = code.find('a', href=True)
                    if link:
                        item['source_url'] = link.get('href', '')
                        item['source_name'] = link.get_text(strip=True)
                    else:
                        # Fallback: extract source text from code
                        source_text = code.get_text(strip=True)
                        source_text = re.sub(r'^.*?Source:?\s*', '', source_text, flags=re.IGNORECASE)
                        # Try to extract URL from text
                        url_match = re.search(r'https?://[^\s<>"]+', source_text)
                        if url_match:
                            item['source_url'] = url_match.group(0)
                            source_text = re.sub(r'https?://[^\s<>"]+', '', source_text).strip()
                        if source_text:
                            item['source_name'] = source_text.strip()
            
            # Look for source in <p><strong>Source:</strong>...<a>...</a></p> (older format)
            elif hasattr(elem, 'name') and elem.name == 'p' and 'Source' in text and not item.get('source_url'):
                # Look for link inside the paragraph
                link = elem.find('a', href=True)
                if link:
                    href = link.get('href', '')
                    # Only use external links (not substack internal links)
                    if href and not href.startswith('#') and 'substack.com' not in href:
                        item['source_url'] = href
                        item['source_name'] = link.get_text(strip=True)
            
            # Also check for source in code tags directly
            elif hasattr(elem, 'name') and elem.name == 'code':
                # Look for link inside code
                link = elem.find('a', href=True)
                if link:
                    item['source_url'] = link.get('href', '')
                    item['source_name'] = link.get_text(strip=True)
                else:
                    # Fallback: extract source text
                    source_text = elem.get_text(strip=True)
                    if 'Source' in source_text:
                        source_text = re.sub(r'^.*?Source:?\s*', '', source_text, flags=re.IGNORECASE)
                        # Try to extract URL from text
                        url_match = re.search(r'https?://[^\s<>"]+', source_text)
                        if url_match:
                            item['source_url'] = url_match.group(0)
                            source_text = re.sub(r'https?://[^\s<>"]+', '', source_text).strip()
                        if source_text:
                            item['source_name'] = source_text.strip()
            
            i += 1
        
        return item
    
    def _extract_item_details(self, start_elem, item: Dict, section_end=None) -> Dict:
        """Extract What Happened, Why It Matters, and Source from elements following an item title."""
        current = start_elem.next_sibling if hasattr(start_elem, 'next_sibling') else None
        found_what_happened = False
        found_why_matters = False
        
        while current:
            # Stop if we hit the next section or item
            if current == section_end:
                break
            
            if not hasattr(current, 'get_text'):
                current = getattr(current, 'next_sibling', None)
                continue
            
            text = current.get_text(strip=True)
            
            # Check if we've hit the next item (new title)
            if hasattr(current, 'name'):
                if current.name in ['h3', 'h4']:
                    if not re.match(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News', text):
                        break  # Next item found
                elif current.name in ['strong', 'b'] and len(text) > 20:
                    if not re.match(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨]', text):
                        break  # Next item found
            
            # Look for "What Happened:" or "What Happened"
            if ('What Happened' in text or 'What Happened:' in text) and not found_what_happened:
                # Extract text after "What Happened:"
                what_happened = re.sub(r'^.*?What Happened:?\s*', '', text, flags=re.IGNORECASE)
                if what_happened and len(what_happened) > 10:
                    item['item_what_happened'] = what_happened
                    found_what_happened = True
                else:
                    # Check next element
                    next_p = getattr(current, 'find_next_sibling', lambda x: None)('p')
                    if next_p:
                        next_text = next_p.get_text(strip=True)
                        if next_text:
                            item['item_what_happened'] = next_text
                            found_what_happened = True
            
            # Look for "Why It Matters:" or "Why It Matters"
            elif ('Why It Matters' in text or 'Why It Matters:' in text) and not found_why_matters:
                why_matters = re.sub(r'^.*?Why It Matters:?\s*', '', text, flags=re.IGNORECASE)
                if why_matters and len(why_matters) > 10:
                    item['item_why_it_matters'] = why_matters
                    found_why_matters = True
                else:
                    next_p = getattr(current, 'find_next_sibling', lambda x: None)('p')
                    if next_p:
                        next_text = next_p.get_text(strip=True)
                        if next_text:
                            item['item_why_it_matters'] = next_text
                            found_why_matters = True
            
            # Look for source (in code blocks or "Source:" text)
            elif hasattr(current, 'name') and (current.name == 'code' or 'Source:' in text or 'Source' in text):
                source_text = re.sub(r'^.*?Source:?\s*', '', text, flags=re.IGNORECASE)
                if source_text:
                    # Extract URL if present
                    url_match = re.search(r'https?://[^\s<>"]+', source_text)
                    if url_match:
                        item['source_url'] = url_match.group(0)
                        source_text = re.sub(r'https?://[^\s<>"]+', '', source_text).strip()
                    if source_text:
                        item['source_name'] = source_text.strip()
            
            # Move to next sibling
            current = getattr(current, 'next_sibling', None)
        
        return item
    
    def _extract_items_without_sections(self, content) -> List[Dict]:
        """Fallback: extract items when section structure isn't clear."""
        items = []
        current_section = ""
        
        # Look for all h3 elements with header-anchor-post class (item titles)
        all_h3s = content.find_all('h3', class_=re.compile('header-anchor-post'))
        
        for h3 in all_h3s:
            title_text = h3.get_text(strip=True)
            
            # Skip if it's a section header
            if re.match(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News', title_text):
                current_section = title_text
                continue
            
            # Extract item_section_type from <strong> tag if present
            item_section_type = current_section  # Default to current_section
            item_section = ""  # Will be h3 text excluding strong
            
            strong_tag = h3.find('strong')
            if strong_tag:
                # Extract text from strong tag as item_section_type
                item_section_type = strong_tag.get_text(strip=True)
                # Extract h3 text excluding the strong tag as item_section
                h3_copy = BeautifulSoup(str(h3), 'lxml').find('h3')
                if h3_copy:
                    strong_in_copy = h3_copy.find('strong')
                    if strong_in_copy:
                        strong_in_copy.decompose()  # Remove strong tag
                    item_section = h3_copy.get_text(strip=True)
            else:
                # No strong tag, use full text as item_section
                item_section = title_text
            
            # Skip if too short (likely not an item title)
            if len(item_section) < 20 and len(title_text) < 20:
                continue
            
            # Create new item
            item = {
                'item_section_type': item_section_type,
                'item_section': item_section,
                'item_what_happened': '',
                'item_why_it_matters': '',
                'source_name': '',
                'source_url': ''
            }
            
            # Get following siblings to extract details
            current = h3.next_sibling
            found_what_happened = False
            found_why_matters = False
            
            while current:
                # Stop if we hit the next item (another h3)
                if hasattr(current, 'name') and current.name == 'h3':
                    classes = current.get('class', [])
                    if 'header-anchor-post' in classes or not classes:
                        break
                
                # Stop if we hit a section header
                if hasattr(current, 'get_text'):
                    text = current.get_text(strip=True)
                    if re.match(r'[🔥🛡️⚖️👊🏼📊🔎💡🚨].*In .+ News', text):
                        current_section = text
                        break
                
                # Extract What Happened
                if not found_what_happened and hasattr(current, 'get_text'):
                    text = current.get_text(strip=True)
                    if 'What Happened' in text:
                        if current.name == 'p':
                            span = current.find('span')
                            if span:
                                item['item_what_happened'] = span.get_text(strip=True)
                                found_what_happened = True
                        else:
                            what_happened = re.sub(r'^.*?What Happened:?\s*', '', text, flags=re.IGNORECASE)
                            if what_happened:
                                item['item_what_happened'] = what_happened
                                found_what_happened = True
                
                # Extract Why It Matters
                if not found_why_matters and hasattr(current, 'get_text'):
                    text = current.get_text(strip=True)
                    if 'Why It Matters' in text:
                        if current.name == 'p':
                            span = current.find('span')
                            if span:
                                item['item_why_it_matters'] = span.get_text(strip=True)
                                found_why_matters = True
                        else:
                            why_matters = re.sub(r'^.*?Why It Matters:?\s*', '', text, flags=re.IGNORECASE)
                            if why_matters:
                                item['item_why_it_matters'] = why_matters
                                found_why_matters = True
                
                # Extract Source (newer format: <pre><code>)
                if hasattr(current, 'name') and current.name == 'pre' and not item.get('source_url'):
                    code = current.find('code')
                    if code:
                        link = code.find('a', href=True)
                        if link:
                            item['source_url'] = link.get('href', '')
                            item['source_name'] = link.get_text(strip=True)
                
                # Extract Source (older format: <p><strong>Source:</strong>...<a>)
                if hasattr(current, 'name') and current.name == 'p' and not item.get('source_url'):
                    text = current.get_text(strip=True) if hasattr(current, 'get_text') else ''
                    if 'Source' in text:
                        link = current.find('a', href=True)
                        if link:
                            href = link.get('href', '')
                            if href and not href.startswith('#') and 'substack.com' not in href:
                                item['source_url'] = href
                                item['source_name'] = link.get_text(strip=True)
                
                # Move to next sibling
                current = getattr(current, 'next_sibling', None)
            
            # Only add item if we have content (what_happened or why_it_matters)
            if item.get('item_what_happened') or item.get('item_why_it_matters'):
                items.append(item)
        
        return items
    
    def _extract_items_from_section(self, section_header, next_section_header, section_name: str) -> List[Dict]:
        """Extract items from a specific section."""
        items = []
        current = section_header.next_sibling
        
        while current and current != next_section_header:
            # Look for item patterns in this section
            # This is a simplified version - we'll use the structure-based approach
            pass
        
        return items
    
    def scrape_post(self, url: str) -> Dict:
        """Main method to scrape a post."""
        html = self.fetch_post_page(url)
        return self.parse_post(html, url)

