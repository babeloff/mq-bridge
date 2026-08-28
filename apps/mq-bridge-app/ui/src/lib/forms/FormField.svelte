<script lang="ts">
  import type { Snippet } from "svelte";
  import MountedNode from "./MountedNode.svelte";

  interface Props {
    label: string;
    description?: string;
    labelFor?: string;
    control?: Node | null;
    wrapperClass?: string;
    required?: boolean;
    children?: Snippet;
  }

  let {
    label,
    description = "",
    labelFor,
    control = null,
    wrapperClass = "",
    required = false,
    children,
  }: Props = $props();
</script>

<div class={`wa-form-row mqb-form-row ${wrapperClass}`.trim()}>
  <label class="wa-form-label mqb-form-label" for={labelFor}>
    <span>{label}</span>
    {#if required}
      <span class="mqb-form-required">*</span>
    {/if}
  </label>
  <div class="wa-form-col mqb-form-col">
    {#if children}
      {@render children()}
    {:else}
      <MountedNode node={control} />
    {/if}
    {#if description}
      <div class="form-description mqb-form-description">{description}</div>
    {/if}
  </div>
</div>
