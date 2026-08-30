<script lang="ts">
  import MountedNode from "./MountedNode.svelte";

  interface Props {
    inputId?: string;
    value: string;
    placeholder?: string;
    valueContent: Node | null;
    removeClassName: string;
  }

  let {
    inputId,
    value,
    placeholder = "Key",
    valueContent,
    removeClassName,
  }: Props = $props();
</script>

<!-- `js-ap-row` / `js-ap-value-wrapper` are the form library's hooks: without them its
     delegated Delete handler finds no row and a key rename never remaps the value's path. -->
<div class="response-header-row mqb-header-row mqb-additional-property-row js-ap-row">
  <input
    id={inputId}
    type="text"
    class="field-input ap-key js-ap-key mqb-additional-property-key"
    placeholder={placeholder}
    value={value}
    data-original-key={value}
  />

  <div class="mqb-additional-property-value js-ap-value-wrapper">
    <MountedNode node={valueContent} />
  </div>

  <button type="button" class={removeClassName}>Delete</button>
</div>
