export async function loadCompressedCsv(url) {
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error(`Failed to fetch CSV: ${res.status}`);
    
    const ds = new DecompressionStream('gzip');
    const decompressed = res.body.pipeThrough(ds);
    const text = await new Response(decompressed).text();

    return d3.csvParse(text);
}