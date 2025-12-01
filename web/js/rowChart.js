import { biasColors } from './shared.js';

export class RowChart {
    constructor(facts, attribute, width, maxItems, updateFunction, title, dim) {
        this.title = title;
        this.dim = dim ? dim : facts.dimension(dc.pluck(attribute));
        this.group = this.dim.group().reduceSum(dc.pluck('count'));

        this.group = removeZeroes(this.group);

        // Fixed height per row; total height will be adjusted dynamically
        const ROW_HEIGHT = 22;
        const MARGINS = { top: 0, right: 10, bottom: 20, left: 10 };

        // Add label to existing container (or create if missing)
        const containerId = 'chart-' + attribute;
        let container = d3.select('#' + containerId);
        if (container.empty()) {
            container = d3.select('#chart-container')
                .append('div')
                .attr('id', containerId);
        }
        container.html(`<div>${title}</div>`);

        function generatePublicationColorMap(facts) {
            const map = {};
            facts.all().forEach(r => {
                if (!map[r.publication]) {
                    map[r.publication] = biasColors[r.bias] || '#c6dbef';
                }
            });
            return map;
        }
        const publicationColorMap = generatePublicationColorMap(facts);

        this.chart = dc.rowChart('#chart-' + attribute)
            .dimension(this.dim)
            .group(this.group)
            .data(d => d.top(maxItems))
            .width(width)
            // initial height; will be overridden in preRender/preRedraw
            .height(Math.max(1, Math.min(maxItems, this.group.all().length)) * ROW_HEIGHT + MARGINS.top + MARGINS.bottom)
            .fixedBarHeight(ROW_HEIGHT)
            .margins(MARGINS)
            .elasticX(true)
            .colors(d => {
                let color = '#c6dbef';
                if ((dc.isGillmor) || (title == 'Publication')) return color;
                if (attribute === 'publication') color = publicationColorMap[d];
                if (attribute === 'bias')        color = biasColors[d];
                return color;
            })
            .label(d => `${d.key}  (${d.value.toLocaleString()})`)
            .labelOffsetX(5)
            .on('filtered', () => updateFunction())
            // Strip x-axis, ticks, and baseline each draw
            .on('pretransition', chart => {
                chart.selectAll('g.axis').remove();
                chart.selectAll('path.domain').remove();
                chart.selectAll('.grid-line').remove();
            });

        // Ensure axis renders nothing if dc tries to create it
        this.chart.xAxis().ticks(0).tickSize(0).tickFormat(() => '');

        // Dynamic total height = visible rows (post zero-filter & current filters), capped at maxItems
        const adjustHeight = () => {
            // Count actual rows that will be rendered (same as .data() callback)
            const visibleData = this.group.top(maxItems);
            const visible = visibleData.length;        
            this.chart.height(Math.max(1, visible) * (ROW_HEIGHT  + 2) + MARGINS.top + MARGINS.bottom);
        };
        this.chart.on('preRender', adjustHeight);


        function removeZeroes(group) {
            const keep = d => d.value > 0 && d.key !== '' && d.key != null;
            return {
                all: () => group.all().filter(keep),
                top: n => group.top(Infinity).filter(keep).slice(0, n)
            };
        }
    }
}