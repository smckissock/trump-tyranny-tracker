import {RowChart} from './rowChart.js'; 
import {formatDate, addCommas, scrollToTop, biasColors} from './shared.js';
import {loadCompressedCsv} from './fileUtil.js';

export class TrumpTyrannyTracker {

    constructor() {
        if (window.location.hostname === '127.0.0.1') 
            document.title = 'Trump Tyranny Tracker DEV';
    
        this.stories = null; // defer loading
        window.ttt = this;

        // Defer heavy data load until after first paint
        const startDataLoad = () => requestAnimationFrame(() => this.getData());
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', startDataLoad, { once: true });
        } else {
            startDataLoad();
        }
    }

    // configure() {
    //     dc.leftWidth = 170;
    // }
   
    async getData() {      
        const overlay = document.getElementById('loading-overlay');
        overlay.classList.replace('loading-hidden','loading-visible');

        // Get base path for GitHub Pages (works locally too)
        const pathname = window.location.pathname;
        const basePath = pathname.split('/').slice(0, -1).join('/') || '';
        const dataPath = basePath ? `${basePath}/data/` : 'data/';

        console.log(dataPath + 'world_stories.csv.gz')
        const downloadedStories = await loadCompressedCsv(dataPath + 'story.csv.gz');

        // Filter out glitchy rows with empty title
        const stories = downloadedStories.filter(story => story.title !== '');

        stories.forEach(story => {
            story.count = 1;
            story.date = new Date(story.publishDate);
            if (story.title == '') 
                story.title = 'Link to story';
            
            const languageMap = {
                'en': 'English',
                'es': 'Spanish',
                'pt': 'Portuguese',
                'fr': 'French'
            };
            if (story.language && languageMap[story.language])
                story.language = languageMap[story.language];
        });

        // try {
        //     const meta = await fetch(dataPath + "stories.meta.json").then(r => r.json());
        //     const generated = meta?.stories?.generated_at;
        //     if (generated) {
        //         const dt = new Date(generated);
        //         const options = { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit' };
        //         const text = `Updated ${dt.toLocaleString('en-US', options)}`;
        //         const el = document.getElementById("updated-stories");
        //         if (el) el.textContent = text;
        //     }
        // } catch (e) {
        //     console.warn("Could not load stories.meta.json timestamp", e);  
        // }

        this.facts = crossfilter(stories);
        dc.facts = this.facts;

        // Load entities for search
        await this.loadEntities(dataPath);

        this.setupCharts();
        this.setupEntitySearch();
        dc.renderAll();
        this.refresh();       
        overlay.classList.replace('loading-visible','loading-hidden'); 
    }

    async loadEntities(dataPath) {
        try {
            const response = await fetch(dataPath + 'entities.json.gz');
            const buffer = await response.arrayBuffer();
            const decompressed = pako.inflate(new Uint8Array(buffer), { to: 'string' });
            this.entities = JSON.parse(decompressed);
            console.log(`Loaded ${this.entities.length} entities for search`);
        } catch (e) {
            console.warn('Could not load entities.json.gz, trying uncompressed...', e);
            try {
                const response = await fetch(dataPath + 'entities.json');
                this.entities = await response.json();
                console.log(`Loaded ${this.entities.length} entities for search (uncompressed)`);
            } catch (e2) {
                console.warn('Could not load entities for search', e2);
                this.entities = [];
            }
        }
    }

    setupEntitySearch() {
        if (!this.entities || this.entities.length === 0) {
            console.warn('No entities loaded, search disabled');
            return;
        }

        const searchInput = document.getElementById('entity-search');
        const clearBtn = document.getElementById('clear-search');
        
        if (!searchInput) return;

        // Create a Set of story IDs for fast lookup (convert to numbers for consistent matching)
        this.entityMap = new Map();
        this.entities.forEach(e => {
            // Store IDs as numbers for consistent matching
            const ids = new Set(e.storyIds.map(id => Number(id)));
            this.entityMap.set(e.name, ids);
        });
        

        // Create ID dimension for filtering (convert to number for consistent matching)
        this.idDimension = this.facts.dimension(d => Number(d.id));
        dc.idDimension = this.idDimension;

        // Initialize Fuse.js for fuzzy search
        this.fuse = new Fuse(this.entities, {
            keys: ['name'],
            threshold: 0.3,
            includeScore: true
        });

        // Initialize Awesomplete
        this.awesomplete = new Awesomplete(searchInput, {
            minChars: 2,
            maxItems: 15,
            autoFirst: true
        });

        // Update suggestions on input
        searchInput.addEventListener('input', () => {
            const query = searchInput.value.trim();
            if (query.length < 2) {
                this.awesomplete.list = [];
                return;
            }
            
            const results = this.fuse.search(query);
            this.awesomplete.list = results.slice(0, 15).map(r => r.item.name);
        });

        // Handle selection
        searchInput.addEventListener('awesomplete-selectcomplete', (e) => {
            const entityName = e.text.value;
            this.filterByEntity(entityName);
            clearBtn.style.display = 'inline-block';
        });

        // Clear button
        clearBtn.addEventListener('click', () => {
            this.clearEntityFilter();
            searchInput.value = '';
            clearBtn.style.display = 'none';
        });
    }

    filterByEntity(entityName) {
        const storyIds = this.entityMap.get(entityName);
        if (!storyIds) {
            console.warn(`Entity not found: ${entityName}`);
            return;
        }
        
        console.log(`Filtering by entity: ${entityName} (${storyIds.size} stories)`);
        
        // Store selected entity name
        this.selectedEntity = entityName;
        
        // Filter to only stories with matching IDs
        this.idDimension.filter(id => storyIds.has(id));
        
        dc.redrawAll();
        this.refresh();
    }

    clearEntityFilter() {
        if (this.idDimension) {
            this.selectedEntity = null;
            this.idDimension.filterAll();
            dc.redrawAll();
            this.refresh();
        }
    }

    setupCharts() {
        const chartWidth = 160

        dc.refresh = this.refresh;
        dc.rowCharts = [
            new RowChart(this.facts, 'sourceName',  chartWidth, 200, this.refresh, 'Publication', null, true),
            new RowChart(this.facts, 'authors',     chartWidth, 200, this.refresh, 'Author', null, true)
        ];
        this.setupMonthChart();

        this.sectionDimension = this.facts.dimension(d => d.itemSectionType || '');
        dc.sectionDimension = this.sectionDimension;
        
        this.setupSectionPanels();
        this.listStories();
    }

    setupMonthChart() {
        return

        this.monthDimension = this.facts.dimension(d => d3.timeMonth(d.date));
        this.monthGroup = this.monthDimension.group().reduceCount();

        const monthEl = document.getElementById('chart-month');
        const height = monthEl?.clientHeight || 120;

        const leftWidth = 550;
        const width = window.innerWidth - leftWidth; 

        const minD = this.monthDimension.bottom(1)[0]?.date;
        const minMonth = d3.timeMonth.floor(minD);
        const maxMonth = d3.timeMonth.offset(d3.timeMonth.floor(new Date()), 0);
        
        const maxValue = this.monthGroup.top(1)[0]?.value || 100;

        // Generate array of all months in range
        const allMonths = d3.timeMonths(minMonth, d3.timeMonth.offset(maxMonth, 1));

        this.monthChart = new dc.BarChart('#chart-month');
        this.monthChart
            .width(width)
            .height(height)
            .dimension(this.monthDimension)
            .group(this.monthGroup)
            .x(d3.scaleBand().domain(allMonths)) 
            .xUnits(dc.units.ordinal) 
            .y(d3.scalePow().exponent(0.5).domain([0, maxValue])) 
            .centerBar(false) 
            .colors(['#c6dbef'])
            .barPadding(0.1) 
            .elasticY(true)
            .brushOn(true)
            .margins({ top: 10, right: 10, bottom: 20, left: 40 })
            .on('filtered', () => this.refresh()).on('renderlet', function(chart) {
                chart.selectAll('.bar-label').remove();
            });
        this.monthChart.xAxis().tickFormat(d3.timeFormat('%b'));
        this.monthChart.yAxis().ticks(3);

        dc.monthDimension = this.monthDimension;
        dc.monthChart = this.monthChart;

        // Change width when window resizes
        window.addEventListener('resize', () => {
            const newWidth = window.innerWidth - leftWidth;
            this.monthChart
                .width(newWidth)
                .rescale() 
                .redraw(); 
        });
    }

    setupSectionPanels() {
        const sectionContainer = d3.select('#chart-section');        
        sectionContainer.html(''); 
        
        TrumpTyrannyTracker.sections.forEach((section, index) => {
            const panel = sectionContainer.append('div')
                .attr('class', 'section-panel')
                .attr('data-section', section.name)
                .on('click', function() {
                    const clickedSection = d3.select(this).attr('data-section');
                    
                    // Radio button behavior - always select clicked, can't deselect
                    d3.selectAll('.section-panel').classed('active', false);
                    d3.select(this).classed('active', true);
                    dc.sectionDimension.filter(clickedSection);
                    
                    dc.redrawAll();
                    window.ttt.refresh();
                });
            
            panel.append('div')
                .attr('class', 'section-name')
                .text(section.name);
        });
        
        // Select first section by default
        this.selectSection(0);
    }
    
    selectSection(index) {
        const section = TrumpTyrannyTracker.sections[index];
        if (section) {
            d3.selectAll('.section-panel').classed('active', false);
            d3.select(d3.selectAll('.section-panel').nodes()[index]).classed('active', true);
            dc.sectionDimension.filter(section.name);
        }
    }

    refresh() {          
        let filters = [];
        const filterTypes = [];
        dc.rowCharts.forEach(rowChart => {
            const chartFilters = rowChart.chart.filters();
            if (chartFilters.length > 0) {
                filterTypes.push({
                    name: rowChart.title,  
                    filters: chartFilters
                });
            }
        });
        
        // Section filter is always active (radio button behavior), so don't show a filter box for it

        // Add month (range) filter if active
        if (dc.monthDimension) {
            const rng = dc.monthDimension.currentFilter(); // dc.filterRange or null
            if (rng && rng[0] && rng[1]) {
                const fmt = d3.timeFormat('%b %Y');
                const label = `${fmt(rng[0])} – ${fmt(rng[1])}`;
                filterTypes.push({
                    name: 'Month',
                    filters: [label]
                });
            }
        }

        const filterBoxes = filterTypes.map(filterType => `
            <div class='filter-box' data-filter-name='${filterType.name}'>
                <button class='filter-box-close'>✕</button>
                <div class='filter-box-title'>${filterType.name}</div>
                <div class='filter-box-values'>${filterType.filters.join(', ')}</div>
            </div>
        `).join('');
        
        const hasActiveFilters = filterTypes.length > 0;
            
        const stories = dc.facts.allFiltered().length;
        d3.select('#case-count').text(`${addCommas(stories)} cases`);
        d3.select('#selected-entity').text(window.ttt.selectedEntity || '');
        d3.select('#filters')
            .html(`
                <span class='case-filters'>${filters.join(', ')}</span>
                <div class='filter-boxes-container'>${filterBoxes}</div>
            `);

        if (hasActiveFilters) {
            // Individual filter box close buttons
            d3.selectAll('.filter-box-close').on('click', function(event) {
                event.stopPropagation();
                const filterName = d3.select(this.parentNode).attr('data-filter-name');
                
                // Find and clear the appropriate filter
                const rowChart = dc.rowCharts.find(rc => rc.title === filterName);
                if (rowChart) {
                    rowChart.chart.filterAll();
                } else if (filterName === 'Section') {
                    window.ttt.selectSection(0);
                } else if (filterName === 'Month' && dc.monthChart) {
                    dc.monthChart.filterAll();
                }
                
                dc.redrawAll();
                window.ttt.refresh();
            });
        }

        d3.select('#add-story').on('click', () => {
            console.log('submitting story');
            this.addStory('joe').then(console.log);  
        });

        dc.redrawAll();
        scrollToTop('#chart-publication');
        scrollToTop('#chart-list');
        window.ttt.listStories();
    }

    listStories() {
        const storiesToShow = 100;
        
        function storyResult(d) {
            const url = d.sourceUrl || '#';
            const hasLink = d.sourceUrl && d.sourceUrl.length > 0;
            const itemTitle = d.itemSection || '';
            const whatHappened = d.itemWhatHappened || '';
            const whyItMatters = d.itemWhyItMatters || '';
            const source = d.sourceName || '';
            const authors = d.authors || '';
            const articleTitle = d.title || '';
            const dateStr = d.publishDate ? formatDate(new Date(d.publishDate)) : '';
            
            // Skip stories with blocked content
            if (articleTitle.includes('Access to this page has been denied')) return '';
            
            return `
              <div class="story-card">
                ${itemTitle ? `<h2 class="story-card-title">${itemTitle}</h2>` : ''}
                
                ${whatHappened ? `
                <div class="story-card-section">
                  <p><strong>What Happened:</strong> ${whatHappened}</p>
                </div>` : ''}
                
                ${whyItMatters ? `
                <div class="story-card-section">
                  <p><strong>Why It Matters:</strong> ${whyItMatters}</p>
                </div>` : ''}
                
                <div class="story-card-source" ${hasLink ? `onclick="window.open('${url}', '_blank', 'noopener')"` : ''}>
                  ${d.image ? `
                  <img
                    class="story-card-image"
                    src="${d.image}"
                    onload="this.classList.add('loaded')"
                    onerror="this.style.display='none'"
                  >` : ''}
                  <div class="story-card-source-info">
                    <span class="story-card-source-label">Source: <em>${source}</em></span>
                    ${articleTitle ? `<span class="story-card-source-title">${articleTitle}</span>` : ''}
                    <span class="story-card-source-meta">${authors}${authors && dateStr ? ' · ' : ''}${dateStr}</span>
                  </div>
                </div>
              </div>
            `;
        }

        const filtered = this.facts.allFiltered();
        
        let html = filtered
            .sort((a, b) => new Date(b.publishDate || 0) - new Date(a.publishDate || 0))
            .slice(0, storiesToShow)
            .map(d => storyResult(d))
            .join('');

        d3.select('#chart-list')
            .html(html);
    }   

    async addStory(story) {
        const response = await fetch(`/api/echo?name=${encodeURIComponent(story)}`, {
            method: 'GET',
            headers: { 'Content-Type': 'text/plain' }
        });
        const data = await response.json();
        return data.response;
    }

    static sections = [
        {
            "name": "⚖️ In Weaponization of Institutions News",
        },
        {
            "name": "🛡️ In Power Consolidation News",
        },
        {
            "name": "👊🏼 In Resistance News",
        },
        {
            "name": "🔥 In Corruption News",
        }
    ];
    
    static bureaus = [
        {
            "name": "Africa",
            "abbreviation": "AFR",
            "description": "Manages USAID programs across sub-Saharan Africa"
        },
        {
            "name": "Asia",
            "abbreviation": "ASIA",
            "description": "Oversees development and humanitarian programs in Asia"
        },
        {
            "name": "Europe & Eurasia",
            "abbreviation": "E&E",
            "description": "Supports democratic and economic transitions in Europe and Eurasia"
        },
        {
            // "name": "Latin America and the Caribbean",
            "name": "Latin Am & Carib",
            "abbreviation": "LAC",
            "description": "Promotes economic growth and democracy in Latin America and the Caribbean"
        },
        {
            "name": "Middle East",
            "abbreviation": "ME",
            "description": "Advances stability and prosperity in the Middle East"
        }
    ]
}

const ttt = new TrumpTyrannyTracker();
