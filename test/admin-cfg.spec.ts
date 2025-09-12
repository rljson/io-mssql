// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be

import { describe, expect, it } from 'vitest';

import { adminCfg } from '../src/admin-cfg';

describe('adminCfg', () => {
  it('should have the correct user', () => {
    expect(adminCfg.user).toBe('sa');
  });

  it('should have the correct password', () => {
    expect(adminCfg.password).toBe('Password123!');
  });

  it('should have the correct server', () => {
    expect(adminCfg.server).toBe('localhost');
  });

  it('should have the correct database', () => {
    expect(adminCfg.database).toBe('master');
  });

  it('should have options defined', () => {
    expect(adminCfg.options).toBeDefined();
    expect(typeof adminCfg.options).toBe('object');
  });

  it('should have encrypt set to false in options', () => {
    expect(adminCfg.options.encrypt).toBe(false);
  });

  it('should have trustServerCertificate set to true in options', () => {
    expect(adminCfg.options.trustServerCertificate).toBe(true);
  });
});
