// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';
let counter = 0;
const { shouldUpdateGoldens } = await import('./goldens.ts');
console.log(typeof shouldUpdateGoldens());
while (typeof shouldUpdateGoldens() !== 'boolean') {
  await new Promise(resolve => setTimeout(resolve, 100));
  console.log('Waiting for shouldUpdateGoldens to load...');
console.log(typeof shouldUpdateGoldens());
  counter ++;
  if(counter > 150){
    throw new Error('Timeout waiting for shouldUpdateGoldens to load');
  }
}

describe('shouldUpdateGoldens', async () => {
  it('should be true when process.argv contains --update-goldens', async () => {    

    const backup = process.env['UPDATE_GOLDENS'];
    (process.env as any).UPDATE_GOLDENS = true;
    expect(shouldUpdateGoldens()).toBe(true);
    (process.env as any).UPDATE_GOLDENS = false;
    expect(shouldUpdateGoldens()).toBe(false);
    (process.env as any).UPDATE_GOLDENS = backup;
  });
});
