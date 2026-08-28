<script lang="ts">
  import MountedNode from "./MountedNode.svelte";

  interface Props {
    inputId?: string;
    value: string;
    placeholder?: string;
    valueContent: Node | null;
    removeClassName: string;
    label?: string;
  }

  let {
    inputId,
    value,
    placeholder = "",
    valueContent,
    removeClassName,
    label = "Route Name",
  }: Props = $props();
</script>

<div class="ap-row js-ap-row mqb-ap-row">
  <div class="ap-row-header mqb-ap-row-header">
    <div class="ap-row-key mqb-ap-row-key">
      <label class="wa-form-label mqb-form-label" for={inputId}>{label}</label>
      <input
        id={inputId}
        type="text"
        class="ap-key js-ap-key field-input"
        placeholder={placeholder}
        value={value}
        data-original-key={value}
      />
    </div>
    <button type="button" class={removeClassName}>Remove Route</button>
  </div>

  <div class="ap-row-body mqb-ap-row-body js-ap-value-wrapper">
    <MountedNode node={valueContent} />
  </div>
</div>
