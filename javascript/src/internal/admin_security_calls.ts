// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The admin client's security RPCs (private): the ACL calls, the client quota
// calls, the SCRAM credential calls and the delegation-token calls, as
// src/internal/admin_calls.ts has the others. Each checks the
// call's form and its arguments, runs the RPC and builds its Result from the
// native result's futures, as Python's confluent_kafka/admin/_conversions.py
// composes them. An ACL binding or filter crosses the addon as Python's
// seven-tuple, each enum as Java's byte code; a quota entity as its entries,
// a principal as [principalType, name]. Where Java stores a null and reads it
// only later, on its own thread, the call reports it with a `TypeError`, a
// deviation the admin package's documentation lists.

// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way
import native = require('./native');
import { kind } from './args';
import { adminFuture, adminFutures } from './admin_result';
import type { AdminHandle } from './admin_result';
import { instancesOf, optionSetters, optionsFirst, rpcForms, topicNames, voidOf } from './admin_calls';
import { JavaForms } from './args';
import { AccessControlEntry } from '../common/acl/access_control_entry';
import { AccessControlEntryFilter } from '../common/acl/access_control_entry_filter';
import { AclBinding } from '../common/acl/acl_binding';
import { AclBindingFilter } from '../common/acl/acl_binding_filter';
import { AclBindingFilterMap } from '../common/acl/acl_binding_filter_map';
import { AclBindingMap } from '../common/acl/acl_binding_map';
import { AclOperation } from '../common/acl/acl_operation';
import { AclPermissionType } from '../common/acl/acl_permission_type';
import type { ApiError } from '../common/errors/api_error';
import type { KafkaFuture } from '../common/kafka_future';
import { PatternType } from '../common/resource/pattern_type';
import { ResourcePattern } from '../common/resource/resource_pattern';
import { ResourcePatternFilter } from '../common/resource/resource_pattern_filter';
import { ResourceType } from '../common/resource/resource_type';
import { CreateAclsOptions } from '../admin/create_acls_options';
import { CreateAclsResult } from '../admin/create_acls_result';
import { DeleteAclsOptions } from '../admin/delete_acls_options';
import { DeleteAclsResult } from '../admin/delete_acls_result';
import { DescribeAclsOptions } from '../admin/describe_acls_options';
import { DescribeAclsResult } from '../admin/describe_acls_result';
import { ClientQuotaAlteration } from '../common/quota/client_quota_alteration';
import { ClientQuotaEntity } from '../common/quota/client_quota_entity';
import { ClientQuotaEntityMap } from '../common/quota/client_quota_entity_map';
import { ClientQuotaFilter } from '../common/quota/client_quota_filter';
import { KafkaPrincipal } from '../common/security/auth/kafka_principal';
import { DelegationToken } from '../common/security/token/delegation/delegation_token';
import { TokenInformation } from '../common/security/token/delegation/token_information';
import { AlterClientQuotasOptions } from '../admin/alter_client_quotas_options';
import { AlterClientQuotasResult } from '../admin/alter_client_quotas_result';
import { AlterUserScramCredentialsOptions } from '../admin/alter_user_scram_credentials_options';
import { AlterUserScramCredentialsResult } from '../admin/alter_user_scram_credentials_result';
import { CreateDelegationTokenOptions } from '../admin/create_delegation_token_options';
import { CreateDelegationTokenResult } from '../admin/create_delegation_token_result';
import { DescribeClientQuotasOptions } from '../admin/describe_client_quotas_options';
import { DescribeClientQuotasResult } from '../admin/describe_client_quotas_result';
import { DescribeDelegationTokenOptions } from '../admin/describe_delegation_token_options';
import { DescribeDelegationTokenResult } from '../admin/describe_delegation_token_result';
import { DescribeUserScramCredentialsOptions } from '../admin/describe_user_scram_credentials_options';
import { DescribeUserScramCredentialsResult } from '../admin/describe_user_scram_credentials_result';
import { ExpireDelegationTokenOptions } from '../admin/expire_delegation_token_options';
import { ExpireDelegationTokenResult } from '../admin/expire_delegation_token_result';
import { RenewDelegationTokenOptions } from '../admin/renew_delegation_token_options';
import { RenewDelegationTokenResult } from '../admin/renew_delegation_token_result';
import { ScramCredentialInfo } from '../admin/scram_credential_info';
import { ScramMechanism } from '../admin/scram_mechanism';
import { UserScramCredentialAlteration } from '../admin/user_scram_credential_alteration';
import { UserScramCredentialDeletion } from '../admin/user_scram_credential_deletion';
import { UserScramCredentialUpsertion } from '../admin/user_scram_credential_upsertion';
import { UserScramCredentialsDescription } from '../admin/user_scram_credentials_description';

const CREATE_ACLS_FORMS = rpcForms('createAcls', ['acls'], CreateAclsOptions);
const DESCRIBE_ACLS_FORMS = rpcForms('describeAcls', ['filter'], DescribeAclsOptions);
const DELETE_ACLS_FORMS = rpcForms('deleteAcls', ['filters'], DeleteAclsOptions);
const DESCRIBE_CLIENT_QUOTAS_FORMS = rpcForms('describeClientQuotas', ['filter'], DescribeClientQuotasOptions);
const ALTER_CLIENT_QUOTAS_FORMS = rpcForms('alterClientQuotas', ['entries'], AlterClientQuotasOptions);
const DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS = new JavaForms('describeUserScramCredentials', [[], ['users'], ['users', 'options']]);
const ALTER_USER_SCRAM_CREDENTIALS_FORMS = rpcForms('alterUserScramCredentials', ['alterations'], AlterUserScramCredentialsOptions);
const CREATE_DELEGATION_TOKEN_FORMS = rpcForms('createDelegationToken', [], CreateDelegationTokenOptions);
const RENEW_DELEGATION_TOKEN_FORMS = rpcForms('renewDelegationToken', ['hmac'], RenewDelegationTokenOptions);
const EXPIRE_DELEGATION_TOKEN_FORMS = rpcForms('expireDelegationToken', ['hmac'], ExpireDelegationTokenOptions);
const DESCRIBE_DELEGATION_TOKEN_FORMS = rpcForms('describeDelegationToken', [], DescribeDelegationTokenOptions);

/** An ACL binding or filter as the addon passes it. */
type AclFields = [number, string | null, number, string | null, string | null, number, number];

/**
 * A binding, for the addon.
 *
 * @param b - the binding
 * @returns its seven fields
 */
function aclBindingFields(b: AclBinding): AclFields {
  const p = b.pattern();
  const e = b.entry();
  return [
    p.resourceType().code(),
    p.name(),
    p.patternType().code(),
    e.principal(),
    e.host(),
    e.operation().code(),
    e.permissionType().code(),
  ];
}

/**
 * A filter, for the addon.
 *
 * @param f - the filter
 * @returns its seven fields
 */
function aclBindingFilterFields(f: AclBindingFilter): AclFields {
  const p = f.patternFilter();
  const e = f.entryFilter();
  return [
    p.resourceType().code(),
    p.name(),
    p.patternType().code(),
    e.principal(),
    e.host(),
    e.operation().code(),
    e.permissionType().code(),
  ];
}

/**
 * A binding the addon read.
 *
 * @param raw - its seven fields
 * @returns the binding
 */
export function aclBindingOf(raw: unknown): AclBinding {
  const [rt, name, pt, principal, host, op, perm] = raw as AclFields;
  return new AclBinding({
    pattern: new ResourcePattern({ resourceType: ResourceType.fromCode(rt), name: name as string, patternType: PatternType.fromCode(pt) }),
    entry: new AccessControlEntry({
      principal: principal as string,
      host: host as string,
      operation: AclOperation.fromCode(op),
      permissionType: AclPermissionType.fromCode(perm),
    }),
  });
}

/**
 * A filter the addon read.
 *
 * @param raw - its seven fields
 * @returns the filter
 */
function aclBindingFilterOf(raw: unknown): AclBindingFilter {
  const [rt, name, pt, principal, host, op, perm] = raw as AclFields;
  return new AclBindingFilter({
    patternFilter: new ResourcePatternFilter({ resourceType: ResourceType.fromCode(rt), name, patternType: PatternType.fromCode(pt) }),
    entryFilter: new AccessControlEntryFilter({
      principal,
      host,
      operation: AclOperation.fromCode(op),
      permissionType: AclPermissionType.fromCode(perm),
    }),
  });
}

/**
 * A filter's results the addon read, `[{ binding, error }]`.
 *
 * @param raw - the results
 * @returns the results
 */
function filterResultsOf(raw: unknown): DeleteAclsResult.FilterResults {
  const rows = raw as Array<{ binding: unknown; error: unknown }>;
  return DeleteAclsResult.FilterResults.internalCreate(
    rows.map((r) =>
      DeleteAclsResult.FilterResult.internalCreate(r.binding === null ? null : aclBindingOf(r.binding), r.error as ApiError | null),
    ),
  );
}

/**
 * `createAcls(acls)` and `createAcls(acls, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function createAcls(handle: AdminHandle, args: unknown, count: number): CreateAclsResult {
  CREATE_ACLS_FORMS.match(args, count);
  // Java dereferences each binding (toFilter) before it reads the options.
  const acls = instancesOf(CREATE_ACLS_FORMS.value(args, 'acls'), 'acls', AclBinding).map(aclBindingFields);
  const box = native.adminCreateAcls(handle, acls, optionSetters(CREATE_ACLS_FORMS, args, CreateAclsOptions));
  const futures = adminFutures(handle, box, 'values', aclBindingOf, voidOf) ?? [];
  return CreateAclsResult.internalCreate(new AclBindingMap<KafkaFuture<void>>(futures));
}

/**
 * `describeAcls(filter)` and `describeAcls(filter, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeAcls(handle: AdminHandle, args: unknown, count: number): DescribeAclsResult {
  DESCRIBE_ACLS_FORMS.match(args, count);
  // Java's first dereference is the filter's (isUnknown).
  const filter = DESCRIBE_ACLS_FORMS.value(args, 'filter');
  if (filter === null) throw new TypeError('filter must not be null');
  if (!(filter instanceof AclBindingFilter)) throw new TypeError(`filter must be an AclBindingFilter, not ${kind(filter)}`);
  const box = native.adminDescribeAcls(
    handle,
    aclBindingFilterFields(filter),
    optionSetters(DESCRIBE_ACLS_FORMS, args, DescribeAclsOptions),
  );
  return DescribeAclsResult.internalCreate(adminFuture(handle, box, 'values', null, (raw) => (raw as unknown[]).map(aclBindingOf)));
}

/**
 * `deleteAcls(filters)` and `deleteAcls(filters, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function deleteAcls(handle: AdminHandle, args: unknown, count: number): DeleteAclsResult {
  DELETE_ACLS_FORMS.match(args, count);
  // Java dereferences each filter (DeleteAclsRequest.deleteAclsFilter) before
  // it reads the options.
  const filters = instancesOf(DELETE_ACLS_FORMS.value(args, 'filters'), 'filters', AclBindingFilter).map(aclBindingFilterFields);
  const box = native.adminDeleteAcls(handle, filters, optionSetters(DELETE_ACLS_FORMS, args, DeleteAclsOptions));
  const futures = adminFutures(handle, box, 'values', aclBindingFilterOf, filterResultsOf) ?? [];
  return DeleteAclsResult.internalCreate(new AclBindingFilterMap<KafkaFuture<DeleteAclsResult.FilterResults>>(futures));
}

// ---- client quotas ------------------------------------------------------------

/** A quota entity's entries as the addon passes them. */
type EntityFields = Array<[string, string | null]>;

/**
 * An entity, for the addon: its entries.
 *
 * @param entity - the entity
 * @param what - what the entity is, for the error
 * @returns its entries
 * @throws `TypeError` when its entries are null (the deviation the admin
 *   package lists)
 */
function entityFields(entity: ClientQuotaEntity, what: string): EntityFields {
  const entries = entity.entries() as Map<string, string | null> | null;
  if (entries === null) throw new TypeError(`${what} must not be a ClientQuotaEntity with null entries`);
  return [...entries];
}

/**
 * An entity the addon read.
 *
 * @param raw - its entries
 * @returns the entity
 */
function entityOf(raw: unknown): ClientQuotaEntity {
  return new ClientQuotaEntity(new Map(raw as EntityFields));
}

/**
 * `describeClientQuotas(filter)` and `describeClientQuotas(filter, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeClientQuotas(handle: AdminHandle, args: unknown, count: number): DescribeClientQuotasResult {
  DESCRIBE_CLIENT_QUOTAS_FORMS.match(args, count);
  // Java reads the options first (their timeout); it reads the filter when
  // it builds the request.
  optionsFirst(DESCRIBE_CLIENT_QUOTAS_FORMS, args);
  const filter = DESCRIBE_CLIENT_QUOTAS_FORMS.value(args, 'filter');
  if (filter === null) throw new TypeError('filter must not be null');
  if (!(filter instanceof ClientQuotaFilter)) throw new TypeError(`filter must be a ClientQuotaFilter, not ${kind(filter)}`);
  const components = filter.components() as unknown as Array<unknown> | null;
  if (components === null) throw new TypeError('filter must not be a ClientQuotaFilter with null components');
  const rows = components.map((c): [string, number, string | null] => {
    if (c === null) throw new TypeError('filter must not contain a null ClientQuotaFilterComponent');
    const [matchKind, name] = (c as { internalMatch(): [number, string | null] }).internalMatch();
    return [(c as { entityType(): string }).entityType(), matchKind, name];
  });
  const box = native.adminDescribeClientQuotas(
    handle,
    [filter.strict(), rows],
    optionSetters(DESCRIBE_CLIENT_QUOTAS_FORMS, args, DescribeClientQuotasOptions),
  );
  return new DescribeClientQuotasResult(
    adminFuture(handle, box, 'entities', null, (raw) => {
      const map = new ClientQuotaEntityMap<Map<string, number>>();
      for (const [entity, quotas] of raw as Array<[EntityFields, Array<[string, number]>]>) map.set(entityOf(entity), new Map(quotas));
      return map;
    }),
  );
}

/**
 * `alterClientQuotas(entries)` and `alterClientQuotas(entries, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function alterClientQuotas(handle: AdminHandle, args: unknown, count: number): AlterClientQuotasResult {
  ALTER_CLIENT_QUOTAS_FORMS.match(args, count);
  // Java dereferences the entries and each entry (entries.size(),
  // entry.entity()) before it reads the options; it reads each entity's
  // entries and ops when it builds the request.
  const entries = instancesOf(ALTER_CLIENT_QUOTAS_FORMS.value(args, 'entries'), 'entries', ClientQuotaAlteration);
  optionsFirst(ALTER_CLIENT_QUOTAS_FORMS, args);
  const rows = entries.map((entry): [EntityFields, Array<[string, number | null]>] => {
    const entity = entry.entity() as ClientQuotaEntity | null;
    if (entity === null) throw new TypeError('entries must not contain a ClientQuotaAlteration with a null entity');
    const ops = entry.ops() as Array<ClientQuotaAlteration.Op | null> | null;
    if (ops === null) throw new TypeError('entries must not contain a ClientQuotaAlteration with null ops');
    return [
      entityFields(entity, 'entries'),
      ops.map((op): [string, number | null] => {
        if (op === null) throw new TypeError('entries must not contain a ClientQuotaAlteration with a null op');
        const key = op.key() as string | null;
        if (key === null) throw new TypeError('entries must not contain a ClientQuotaAlteration.Op with a null key');
        return [key, op.value()];
      }),
    ];
  });
  const box = native.adminAlterClientQuotas(handle, rows, optionSetters(ALTER_CLIENT_QUOTAS_FORMS, args, AlterClientQuotasOptions));
  const futures = adminFutures(handle, box, 'values', entityOf, voidOf) ?? [];
  return new AlterClientQuotasResult(new ClientQuotaEntityMap<KafkaFuture<void>>(futures));
}

// ---- SCRAM credentials --------------------------------------------------------

/**
 * A description the addon read.
 *
 * @param raw - its name and credential infos
 * @returns the description
 */
function scramDescriptionOf(raw: unknown): UserScramCredentialsDescription {
  const { name, credentialInfos } = raw as { name: string; credentialInfos: Array<[number, number]> };
  return new UserScramCredentialsDescription({
    name,
    credentialInfos: credentialInfos.map(
      ([type, iterations]) => new ScramCredentialInfo({ mechanism: ScramMechanism.fromType(type), iterations }),
    ),
  });
}

/**
 * `describeUserScramCredentials()`, `describeUserScramCredentials(users)` and
 * `describeUserScramCredentials(users, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeUserScramCredentials(handle: AdminHandle, args: unknown, count: number): DescribeUserScramCredentialsResult {
  const form = DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS.match(args, count);
  // Java reads the options first (their timeout); null users are every
  // user, and a null user is skipped, as Java's request builder skips it.
  optionsFirst(DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS, args);
  const given = form === 0 ? null : DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS.value(args, 'users');
  const users = given === null ? null : topicNames(given, 'users').filter((u): u is string => u !== null);
  const box = native.adminDescribeUserScramCredentials(
    handle,
    users,
    optionSetters(DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS, args, DescribeUserScramCredentialsOptions),
  );
  return DescribeUserScramCredentialsResult.internalOfClient(
    () =>
      adminFuture(
        handle,
        box,
        'all',
        null,
        (raw) => new Map((raw as Array<[string, unknown]>).map(([user, d]) => [user, scramDescriptionOf(d)])),
      ),
    () => adminFuture(handle, box, 'users', null, (raw) => raw as string[]),
    (userName) => adminFuture(handle, box, 'description', userName, scramDescriptionOf),
  );
}

/**
 * `alterUserScramCredentials(alterations)` and
 * `alterUserScramCredentials(alterations, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function alterUserScramCredentials(handle: AdminHandle, args: unknown, count: number): AlterUserScramCredentialsResult {
  ALTER_USER_SCRAM_CREDENTIALS_FORMS.match(args, count);
  // Java dereferences each alteration (alteration.user()) before it reads
  // the options.
  const alterations = instancesOf(
    ALTER_USER_SCRAM_CREDENTIALS_FORMS.value(args, 'alterations'),
    'alterations',
    // Its constructor is protected, as Java's is; instanceof needs no access.
    UserScramCredentialAlteration as unknown as abstract new () => UserScramCredentialAlteration,
  );
  optionsFirst(ALTER_USER_SCRAM_CREDENTIALS_FORMS, args);
  const rows = alterations.map((a): [0, string, number, number, Uint8Array, Uint8Array] | [1, string, number] => {
    if (a instanceof UserScramCredentialUpsertion) {
      const info = a.credentialInfo();
      return [0, a.user(), info.mechanism().type(), info.iterations(), a.password(), a.salt()];
    }
    if (a instanceof UserScramCredentialDeletion) return [1, a.user(), a.mechanism().type()];
    throw new TypeError(`alterations must contain only UserScramCredentialUpsertion or UserScramCredentialDeletion, not ${kind(a)}`);
  });
  const box = native.adminAlterUserScramCredentials(
    handle,
    rows,
    optionSetters(ALTER_USER_SCRAM_CREDENTIALS_FORMS, args, AlterUserScramCredentialsOptions),
  );
  const futures = adminFutures(handle, box, 'values', (raw) => raw as string, voidOf) ?? [];
  return new AlterUserScramCredentialsResult(new Map(futures));
}

// ---- delegation tokens --------------------------------------------------------

/**
 * A principal the addon read.
 *
 * @param raw - its type, name and whether it was token-authenticated
 * @returns the principal
 */
function principalOf(raw: unknown): KafkaPrincipal {
  const [principalType, name, tokenAuthenticated] = raw as [string, string, boolean];
  return new KafkaPrincipal({ principalType, name, tokenAuthenticated });
}

/**
 * A token the addon read, built as Java's KafkaAdminClient builds it.
 *
 * @param raw - its eight fields
 * @returns the token
 */
function delegationTokenOf(raw: unknown): DelegationToken {
  const [tokenId, owner, tokenRequester, renewers, issueTimestamp, maxTimestamp, expiryTimestamp, hmac] = raw as [
    string,
    unknown,
    unknown,
    unknown[],
    bigint,
    bigint,
    bigint,
    Buffer,
  ];
  return new DelegationToken({
    tokenInformation: new TokenInformation({
      tokenId,
      owner: principalOf(owner),
      tokenRequester: principalOf(tokenRequester),
      renewers: renewers.map(principalOf),
      issueTimestamp,
      maxTimestamp,
      expiryTimestamp,
    }),
    hmac,
  });
}

/**
 * The HMAC an RPC takes.
 *
 * @param value - the value given
 * @returns the HMAC
 * @throws `TypeError` when it is null (the deviation the admin package
 *   lists) or not a Uint8Array
 */
function hmacOf(value: unknown): Uint8Array {
  if (value === null) throw new TypeError('hmac must not be null');
  if (!(value instanceof Uint8Array)) throw new TypeError(`hmac must be a Uint8Array, not ${kind(value)}`);
  return value;
}

/**
 * `createDelegationToken()` and `createDelegationToken(options)`.
 *
 * @param handle - the client's handle
 * @param options - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function createDelegationToken(handle: AdminHandle, options: unknown, count: number): CreateDelegationTokenResult {
  CREATE_DELEGATION_TOKEN_FORMS.match(options, count);
  // Java's first read is the options' renewers, each dereferenced.
  optionsFirst(CREATE_DELEGATION_TOKEN_FORMS, options);
  const given = CREATE_DELEGATION_TOKEN_FORMS.value(options, 'options');
  if (given instanceof CreateDelegationTokenOptions) {
    const renewers = given.renewers() as Array<KafkaPrincipal | null> | null;
    if (renewers === null) throw new TypeError('CreateDelegationTokenOptions.renewers must not be null');
    if (renewers.includes(null)) throw new TypeError('CreateDelegationTokenOptions.renewers must not contain null');
  }
  const box = native.adminCreateDelegationToken(
    handle,
    optionSetters(CREATE_DELEGATION_TOKEN_FORMS, options, CreateDelegationTokenOptions),
  );
  return CreateDelegationTokenResult.internalCreate(adminFuture(handle, box, 'delegation_token', null, delegationTokenOf));
}

/**
 * `renewDelegationToken(hmac)` and `renewDelegationToken(hmac, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function renewDelegationToken(handle: AdminHandle, args: unknown, count: number): RenewDelegationTokenResult {
  RENEW_DELEGATION_TOKEN_FORMS.match(args, count);
  // Java reads the options first (their timeout); it reads the HMAC when it
  // builds the request.
  optionsFirst(RENEW_DELEGATION_TOKEN_FORMS, args);
  const hmac = hmacOf(RENEW_DELEGATION_TOKEN_FORMS.value(args, 'hmac'));
  const box = native.adminRenewDelegationToken(
    handle,
    hmac,
    optionSetters(RENEW_DELEGATION_TOKEN_FORMS, args, RenewDelegationTokenOptions),
  );
  return RenewDelegationTokenResult.internalCreate(adminFuture(handle, box, 'expiry_timestamp', null, (raw) => raw as bigint));
}

/**
 * `expireDelegationToken(hmac)` and `expireDelegationToken(hmac, options)`.
 *
 * @param handle - the client's handle
 * @param args - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function expireDelegationToken(handle: AdminHandle, args: unknown, count: number): ExpireDelegationTokenResult {
  EXPIRE_DELEGATION_TOKEN_FORMS.match(args, count);
  // As renewDelegationToken.
  optionsFirst(EXPIRE_DELEGATION_TOKEN_FORMS, args);
  const hmac = hmacOf(EXPIRE_DELEGATION_TOKEN_FORMS.value(args, 'hmac'));
  const box = native.adminExpireDelegationToken(
    handle,
    hmac,
    optionSetters(EXPIRE_DELEGATION_TOKEN_FORMS, args, ExpireDelegationTokenOptions),
  );
  return ExpireDelegationTokenResult.internalCreate(adminFuture(handle, box, 'expiry_timestamp', null, (raw) => raw as bigint));
}

/**
 * `describeDelegationToken()` and `describeDelegationToken(options)`.
 *
 * @param handle - the client's handle
 * @param options - the call's argument
 * @param count - how many arguments the call passed
 * @returns the result
 */
export function describeDelegationToken(handle: AdminHandle, options: unknown, count: number): DescribeDelegationTokenResult {
  DESCRIBE_DELEGATION_TOKEN_FORMS.match(options, count);
  // Java reads the options first (their timeout); it reads the owners when
  // it builds the request.
  optionsFirst(DESCRIBE_DELEGATION_TOKEN_FORMS, options);
  const given = DESCRIBE_DELEGATION_TOKEN_FORMS.value(options, 'options');
  if (given instanceof DescribeDelegationTokenOptions && (given.owners() ?? []).includes(null as unknown as KafkaPrincipal)) {
    throw new TypeError('DescribeDelegationTokenOptions.owners must not contain null');
  }
  const box = native.adminDescribeDelegationToken(
    handle,
    optionSetters(DESCRIBE_DELEGATION_TOKEN_FORMS, options, DescribeDelegationTokenOptions),
  );
  return DescribeDelegationTokenResult.internalCreate(
    adminFuture(handle, box, 'delegation_tokens', null, (raw) => (raw as unknown[]).map(delegationTokenOf)),
  );
}
