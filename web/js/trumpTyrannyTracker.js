import {RowChart} from './rowChart.js'; 
import {formatDate, addCommas, scrollToTop, biasColors} from './shared.js';
import {loadCompressedCsv} from './fileUtil.js';

export class TrumpTyrannyTracker {

    constructor() {
        if (window.location.hostname === '127.0.0.1') 
            document.title = 'Trump Tyranny Tracker DEV';

        this.configure();        
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

    configure() {
        dc.leftWidth = 200;
    }
   
    async getData() {      
        const overlay = document.getElementById('loading-overlay');
        overlay.classList.replace('loading-hidden','loading-visible');

        const isLocal = location.hostname.includes('127.0.0.1');
        const dataPath = isLocal ? 'data/' : '/data/';

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

        this.setupCharts();
        dc.renderAll();
        this.refresh();       
        overlay.classList.replace('loading-visible','loading-hidden'); 
    }

    setupCharts() {
        dc.refresh = this.refresh;
        dc.rowCharts = [
            new RowChart(this.facts, 'sourceName', dc.leftWidth, 180, this.refresh, 'Publication', null, true),
            new RowChart(this.facts, 'authors', dc.leftWidth, 200, this.refresh, 'Author', null, true)
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
        
        // Add section filter if active
        if (dc.sectionDimension) {
            const sectionFilter = dc.sectionDimension.currentFilter();
            if (sectionFilter) {
                filterTypes.push({
                    name: 'Section',
                    filters: [sectionFilter]
                });
            }
        }

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
            <div class='filter-box'>
                <div class='filter-box-title'>${filterType.name}</div>
                <div class='filter-box-values'>${filterType.filters.join(', ')}</div>
            </div>
        `).join('');
        
        const hasActiveFilters = filterTypes.length > 0;
        const clearButton = hasActiveFilters ? `<button id='clear-filters' class='clear-button'>Clear Filters</button>` : '';
            
        const stories = dc.facts.allFiltered().length;
        d3.select('#filters')
            .html(`
                <a href='https://trumptyrannytracker.substack.com/' target='_blank' class='nav-link'>From Trump Tyranny Tracker</a>
                ${clearButton}
                <span class='case-count'> ${addCommas(stories)} stories </b></span> &nbsp;
                <span class='case-filters'>${filters.join(', ')}</span>
                <div class='filter-boxes-container'>${filterBoxes}</div>
            `);

        if (hasActiveFilters) {
            d3.select('#clear-filters').on('click', () => {
                // Clear row chart filters
                dc.filterAll();
                
                // Reset to first section (radio button behavior)
                window.ttt.selectSection(0);

                const state = dc.states?.find(d => d.checked);
                if (state) state.checked = false;

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
            const title = d.title || d.itemSection || 'Untitled';
            const excerpt = d.itemWhatHappened || '';
            const source = d.sourceName || '';
            const authors = d.authors || '';
            const dateStr = d.publishDate ? formatDate(new Date(d.publishDate)) : '';
            
            return `
              <div class="story" ${hasLink ? `onclick="window.open('${url}', '_blank', 'noopener')"` : ''}>
                ${d.image ? `
                <img
                  class="story-image"
                  src="${d.image}"
                  onload="this.classList.add('loaded')"
                  onerror="this.style.display='none'"
                  height="90"
                  width="120"
                >` : ''}
                <div class="story-body">
                  <h5 class="story-topic">
                    <strong>${source}</strong> ${dateStr} ${authors}
                  </h5>
                  <h3 class="story-title">${title}</h3>
                  <p class="story-excerpt">${excerpt}</p>
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
