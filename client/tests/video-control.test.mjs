import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import vm from "node:vm";
import ts from "typescript";

function loadVideoModule(name, imports, globals = {}) {
  const source = readFileSync(new URL(`../src/lib/call/${name}.ts`, import.meta.url), "utf8");
  const code = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  const exports = {};
  vm.runInNewContext(code, { exports, require: (name) => imports[name], console, ...globals });
  return exports;
}

test("KF/B1 são controle; alvo recebido antes da câmera é preservado", () => {
  const decoded = [], targets = [], sent = [];
  let keyframes = 0;
  const { setupVideoChannel } = loadVideoModule("video-channel", {
    "@/constants/video": { H264_CHANNEL_LABEL: "h264" },
    "./video-codec": {
      videoSupported: () => true,
      VideoReceiver: class { stream = {}; decode(data) { decoded.push(data); } close() {} },
      VideoSender: class { setTargetBitrate(bps) { targets.push(bps); } requestKeyframe() { keyframes++; } close() {} },
    },
  });
  const dc = { send: (data) => sent.push(data), readyState: "open" };
  const video = setupVideoChannel({ createDataChannel: () => dc });
  dc.onopen();
  assert.deepEqual(sent, ["B1"]);
  dc.onmessage({ data: "B1 750000" });
  video.startSender({});
  dc.onmessage({ data: "KF" });
  dc.onmessage({ data: "B1 0" });
  dc.onmessage({ data: "B1 -1" });
  dc.onmessage({ data: "B1 999999999999999999999" });
  dc.onmessage({ data: "unknown" });
  const au = new ArrayBuffer(16);
  dc.onmessage({ data: au });
  assert.deepEqual(targets, [750000, 0]);
  assert.equal(keyframes, 1);
  assert.deepEqual(decoded, [au]);
  video.close();
});

test("REMB limita o encoder; zero pausa e retomada/KF produzem IDR", async () => {
  const configs = [], encoded = [];
  let nextFrame;
  const reader = {
    read: () => new Promise((resolve) => { nextFrame = resolve; }),
    cancel: async () => { nextFrame?.({ done: true }); },
  };
  const { VideoSender } = loadVideoModule("video-codec", {
    "@/constants/video": {
      VIDEO_CODEC: "avc1.42E01F", VIDEO_WIDTH: 640, VIDEO_HEIGHT: 480,
      VIDEO_FPS: 15, VIDEO_BITRATE: 750000, VIDEO_KEYFRAME_INTERVAL: 15,
    },
  }, {
    VideoEncoder: class {
      encodeQueueSize = 0;
      configure(config) { configs.push(config); }
      encode(_frame, options) { encoded.push(options); }
      close() {}
    },
    MediaStreamTrackProcessor: class { readable = { getReader: () => reader }; },
  });
  const sender = new VideoSender({}, () => {});
  let closedFrames = 0;
  const frame = async () => {
    nextFrame({ value: { close: () => { closedFrames++; } }, done: false });
    await new Promise(setImmediate);
  };
  sender.setTargetBitrate(300000);
  assert.equal(configs.at(-1).bitrate, 300000);
  await frame();
  await frame();
  assert.equal(encoded[0].keyFrame, true);
  assert.equal(encoded[1].keyFrame, false);
  sender.setTargetBitrate(0);
  await frame();
  assert.equal(encoded.length, 2);
  sender.setTargetBitrate(100000);
  await frame();
  assert.equal(encoded.at(-1).keyFrame, true);
  sender.requestKeyframe();
  await frame();
  assert.equal(encoded.at(-1).keyFrame, true);
  sender.setTargetBitrate(2000000);
  assert.equal(configs.at(-1).bitrate, 750000);
  assert.equal(closedFrames, 5);
  sender.close();
});
