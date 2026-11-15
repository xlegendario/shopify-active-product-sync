/*
 * One look for every photograph in a store that asks for it.
 *
 * The photographs come from whichever store shot the pair first - SOSU, ALC,
 * SneakerAsk, GOAT - and every one of them shoots differently: 5:3, square,
 * 1.4:1, white or transparent, with a hard drop shadow or none. Side by side
 * on a collection page that reads as a jumble sale, and choosing another
 * source does not help, because the sources are mixed inside themselves too
 * (half of SOSU's files are called STOCKX_...).
 *
 * So the source stops mattering. Every image is cut free of its background,
 * trimmed, scaled to the same footprint and centred on one square canvas in
 * one colour, with one soft shadow of ours under it. Baked-in shadows are
 * dropped so no pair gets two.
 *
 * It works on packshots: a product on a plain or transparent background,
 * which is nearly everything the library holds. A lifestyle shot with a
 * busy background comes out wrong; those are rare enough to fix by hand.
 *
 * A finished image is kept in Supabase Storage under a hash of its source
 * URL and the style below, so each photograph is worked on once, however
 * many products or runs ask for it. Changing the look means changing STYLE,
 * which gives every image a new name and leaves the old ones alone.
 */
import crypto from "node:crypto";
import sharp from "sharp";

const SIZE = 1600;
const MAX_W = 0.84;          // share of the canvas the product may take, across
const MAX_H = 0.62;          // and up; a boot fills the height, a runner the width
const CENTER_Y = 0.52;
const BACKGROUND = { r: 243, g: 243, b: 243 };   // light grey, chosen 15-11-2026
const TOLERANCE = 26;        // colour distance still counted as background

export const STYLE = "v2-f3f3f3";

const BUCKET = process.env.PHOTO_BUCKET || "storefront-photos";

/*
 * The stores that get these. A name list rather than a field on Merchants,
 * because it is one store today and the look is ours, not something a store
 * switches on for itself.
 */
export function wantsNormalizedPhotos(merchant) {
  const names = String(process.env.NORMALIZE_PHOTO_MERCHANTS || "UNION Amsterdam")
    .split(",")
    .map((name) => name.trim().toLowerCase())
    .filter(Boolean);

  return names.includes(String(merchant?.name || "").trim().toLowerCase());
}

/*
 * How opaque each pixel of the product is, 0-255.
 *
 * With real transparency the alpha channel is the answer, except for the
 * drop shadow many of these files carry at alpha well under 150: that goes,
 * the anti-aliased edge stays.
 *
 * Without it, the background is the colour of the border, and only what is
 * connected to the border counts - a white sole inside the shoe is the shoe,
 * not the background.
 */
function productAlpha(data, width, height) {
  const n = width * height;
  const alpha = new Uint8Array(n);

  let seeThrough = 0;
  for (let i = 0; i < n; i += 1) if (data[i * 4 + 3] < 250) seeThrough += 1;

  if (seeThrough / n > 0.05) {
    for (let i = 0; i < n; i += 1) {
      const kept = ((data[i * 4 + 3] - 150) / (255 - 150)) * 255;
      alpha[i] = kept <= 0 ? 0 : kept >= 255 ? 255 : Math.round(kept);
    }
    return alpha;
  }

  const border = [];
  for (let x = 0; x < width; x += 1) border.push(x, (height - 1) * width + x);
  for (let y = 0; y < height; y += 1) border.push(y * width, y * width + width - 1);

  const median = (channel) => {
    const values = border.map((i) => data[i * 4 + channel]).sort((a, b) => a - b);
    return values[values.length >> 1];
  };
  const bg = [median(0), median(1), median(2)];

  const near = (i) =>
    Math.max(
      Math.abs(data[i * 4] - bg[0]),
      Math.abs(data[i * 4 + 1] - bg[1]),
      Math.abs(data[i * 4 + 2] - bg[2])
    ) <= TOLERANCE;

  const background = new Uint8Array(n);
  const queue = new Int32Array(n);
  let head = 0;
  let tail = 0;

  for (const i of border) {
    if (!background[i] && near(i)) {
      background[i] = 1;
      queue[tail++] = i;
    }
  }

  while (head < tail) {
    const i = queue[head++];
    const x = i % width;
    const neighbours = [x > 0 ? i - 1 : -1, x < width - 1 ? i + 1 : -1, i - width, i + width];

    for (const j of neighbours) {
      if (j < 0 || j >= n || background[j] || !near(j)) continue;
      background[j] = 1;
      queue[tail++] = j;
    }
  }

  for (let i = 0; i < n; i += 1) alpha[i] = background[i] ? 0 : 255;
  return alpha;
}

// One pixel off the edge, so no rim of the old background survives.
function erode(alpha, width, height) {
  const out = new Uint8Array(alpha.length);

  for (let y = 0; y < height; y += 1) {
    for (let x = 0; x < width; x += 1) {
      let min = 255;
      for (let dy = -1; dy <= 1; dy += 1) {
        const yy = y + dy;
        if (yy < 0 || yy >= height) { min = 0; continue; }
        for (let dx = -1; dx <= 1; dx += 1) {
          const xx = x + dx;
          if (xx < 0 || xx >= width) { min = 0; continue; }
          const v = alpha[yy * width + xx];
          if (v < min) min = v;
        }
      }
      out[y * width + x] = min;
    }
  }

  return out;
}

/*
 * The edges the product runs into, as sharp positions ("left", "bottom"...).
 * A product merely close to an edge touches it with a few pixels at most; a
 * cut-off one fills a real stretch of it.
 */
function cutEdges(alpha, width, height) {
  const share = (count, length) => count / length > 0.04;
  let top = 0, bottom = 0, left = 0, right = 0;

  for (let x = 0; x < width; x += 1) {
    if (alpha[x] > 200) top += 1;
    if (alpha[(height - 1) * width + x] > 200) bottom += 1;
  }
  for (let y = 0; y < height; y += 1) {
    if (alpha[y * width] > 200) left += 1;
    if (alpha[y * width + width - 1] > 200) right += 1;
  }

  // sharp wants the horizontal side first: "right bottom", not "bottom right".
  const edges = [];
  if (share(left, height) && !share(right, height)) edges.push("left");
  if (share(right, height) && !share(left, height)) edges.push("right");
  if (share(top, width) && !share(bottom, width)) edges.push("top");
  if (share(bottom, width) && !share(top, width)) edges.push("bottom");

  // Cut on opposite sides too: still a detail, just centred on that axis.
  if (!edges.length && (share(top, width) || share(bottom, width) || share(left, height) || share(right, height))) {
    edges.push("centre");
  }

  return edges;
}

/*
 * A source image in, the normalised JPEG out. Null when nothing that looks
 * like a product was found.
 */
export async function normalizeImage(input) {
  const { data, info } = await sharp(input, { failOn: "none" })
    .rotate()
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });

  const { width, height } = info;

  const raw = productAlpha(data, width, height);
  const eroded = erode(raw, width, height);
  const soft = await sharp(Buffer.from(eroded), { raw: { width, height, channels: 1 } })
    .blur(0.8)
    // sharp turns a one-channel image into sRGB on the way out unless told
    // otherwise, and three bytes a pixel read as one is a striped mess.
    .extractChannel(0)
    .raw()
    .toBuffer();

  if (soft.length !== width * height) throw new Error(`mask came back ${soft.length} bytes for ${width}x${height}`);

  let left = width, top = height, right = -1, bottom = -1;
  for (let y = 0; y < height; y += 1) {
    for (let x = 0; x < width; x += 1) {
      if (soft[y * width + x] <= 40) continue;
      if (x < left) left = x;
      if (x > right) right = x;
      if (y < top) top = y;
      if (y > bottom) bottom = y;
    }
  }

  if (right < 0) return null;

  const rgba = Buffer.from(data);
  for (let i = 0; i < width * height; i += 1) rgba[i * 4 + 3] = soft[i];

  /*
    A detail shot - a heel, the toe box - is cut off by the frame on purpose.
    Shrunk onto the footprint it becomes a square block floating on a shadow.
    So it keeps running into the edge it was cut at: the new background goes
    in, the picture fills the canvas against those edges, and no shadow.
  */
  const cutAt = cutEdges(raw, width, height);

  if (cutAt.length) {
    const flat = await sharp(rgba, { raw: { width, height, channels: 4 } })
      .flatten({ background: BACKGROUND })
      .png()
      .toBuffer();

    return sharp(flat)
      .resize(SIZE, SIZE, { fit: "contain", position: cutAt.join(" "), background: BACKGROUND, kernel: "lanczos3" })
      .flatten({ background: BACKGROUND })
      .jpeg({ quality: 88, mozjpeg: true })
      .toBuffer();
  }

  const cropW = right - left + 1;
  const cropH = bottom - top + 1;
  const scale = Math.min((SIZE * MAX_W) / cropW, (SIZE * MAX_H) / cropH);
  const w = Math.max(1, Math.round(cropW * scale));
  const h = Math.max(1, Math.round(cropH * scale));

  const product = await sharp(rgba, { raw: { width, height, channels: 4 } })
    .extract({ left, top, width: cropW, height: cropH })
    .resize(w, h, { kernel: "lanczos3" })
    .png()
    .toBuffer();

  const x = Math.round((SIZE - w) / 2);
  const y = Math.round(SIZE * CENTER_Y - h / 2);

  const shadow = await sharp(
    Buffer.from(
      `<svg xmlns="http://www.w3.org/2000/svg" width="${SIZE}" height="${SIZE}">` +
        `<ellipse cx="${SIZE / 2}" cy="${y + h * 1.01}" rx="${w * 0.43}" ry="${h * 0.06}" ` +
        `fill="#000" fill-opacity="0.055"/></svg>`
    )
  )
    .blur(28)
    .png()
    .toBuffer();

  return sharp({ create: { width: SIZE, height: SIZE, channels: 3, background: BACKGROUND } })
    .composite([
      { input: shadow, left: 0, top: 0 },
      { input: product, left: x, top: y }
    ])
    .jpeg({ quality: 88, mozjpeg: true })
    .toBuffer();
}

// True for an image this module made, which needs no further treatment.
export function isNormalizedPhoto(url) {
  return String(url || "").includes(`/storage/v1/object/public/${BUCKET}/normalized/`);
}

function storagePath(sourceUrl) {
  const key = crypto.createHash("sha1").update(`${STYLE}|${sourceUrl}`).digest("hex");
  return `normalized/${STYLE}/${key}.jpg`;
}

/*
 * The normalised URL for each source URL, in the same order.
 *
 * all: true asks for every image or none: a product whose gallery would be
 * half old look and half new is worse than leaving it for the next run.
 * Otherwise the images that fail are left out, and when every one fails the
 * sources come back unchanged - a page with photographs in the old look
 * beats a page without any.
 */
export async function normalizedPhotoUrls(urls, { supabaseUrl, supabaseKey, all = false, log = console } = {}) {
  const sources = (urls || []).filter(Boolean);

  if (!sources.length || !supabaseUrl || !supabaseKey) return all ? null : sources;

  const base = supabaseUrl.replace(/\/$/, "");
  const out = [];
  let failed = 0;

  for (const source of sources) {
    const path = storagePath(source);
    const publicUrl = `${base}/storage/v1/object/public/${BUCKET}/${path}`;

    try {
      const known = await fetch(publicUrl, { method: "HEAD" });

      if (known.ok) {
        out.push(publicUrl);
        continue;
      }

      const res = await fetch(source);
      if (!res.ok) throw new Error(`source answered ${res.status}`);

      const jpeg = await normalizeImage(Buffer.from(await res.arrayBuffer()));
      if (!jpeg) throw new Error("no product found in the image");

      const upload = await fetch(`${base}/storage/v1/object/${BUCKET}/${path}`, {
        method: "POST",
        headers: {
          apikey: supabaseKey,
          Authorization: `Bearer ${supabaseKey}`,
          "Content-Type": "image/jpeg",
          "Cache-Control": "max-age=31536000",
          "x-upsert": "true"
        },
        body: jpeg
      });

      if (!upload.ok) throw new Error(`storage answered ${upload.status}: ${(await upload.text()).slice(0, 120)}`);

      out.push(publicUrl);
    } catch (error) {
      failed += 1;
      log.warn?.("PHOTO NORMALIZE FAILED", { source, error: error instanceof Error ? error.message : String(error) });
    }
  }

  if (all) return failed ? null : out;

  return out.length ? out : sources;
}
