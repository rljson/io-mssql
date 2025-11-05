// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';
import { TemplateProject } from '../src/template-project.ts';

describe('TemplateProject', () => {
  it('should validate a template', async () => {
    while (typeof TemplateProject !== 'function') {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    const templateProject = TemplateProject.example;
    expect(templateProject).toBeDefined();
  });
});
