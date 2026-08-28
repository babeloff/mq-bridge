<script lang="ts">
  import MountedNode from "./MountedNode.svelte";

  interface Props {
    label: string;
    description?: string;
    labelFor?: string;
    control?: Node | null;
    checked?: boolean;
    inputId?: string;
    onChange?: (checked: boolean) => void;
    wrapperClass?: string;
  }

  let {
    label,
    description = "",
    labelFor,
    control = null,
    checked = false,
    inputId,
    onChange,
    wrapperClass = "",
  }: Props = $props();
</script>

<div class={`mqb-checkbox-row ${wrapperClass}`.trim()}>
  <div class="mqb-checkbox-control">
    {#if control}
      <MountedNode node={control} />
    {:else}
      <input
        id={inputId}
        class="wa-checkbox"
        type="checkbox"
        checked={checked}
        onchange={(event) => onChange?.((event.currentTarget as HTMLInputElement).checked)}
      />
    {/if}
  </div>

  <div class="mqb-checkbox-copy">
    <label class="mqb-checkbox-label" for={labelFor || inputId}>{label}</label>
    {#if description}
      <div class="mqb-checkbox-description">{description}</div>
    {/if}
  </div>
</div>
