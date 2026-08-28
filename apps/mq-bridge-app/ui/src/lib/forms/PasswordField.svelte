<script lang="ts">
  import FormField from "./FormField.svelte";
  import MountedNode from "./MountedNode.svelte";
  import { defaultPasswordFieldVisibility } from "./password-field-visibility";

  interface Props {
    label: string;
    description?: string;
    labelFor?: string;
    control: Node | null;
    fieldName?: string;
    wrapperClass?: string;
    required?: boolean;
    maskedLabel?: string;
    visibleLabel?: string;
  }

  let {
    label,
    description = "",
    labelFor,
    control,
    fieldName = "",
    wrapperClass = "",
    required = false,
    maskedLabel = "Show",
    visibleLabel = "Hide",
  }: Props = $props();

  let visibleOverride = $state<boolean | null>(null);

  const resolvedInput = $derived.by(() => {
    if (control instanceof HTMLInputElement) {
      return control;
    }

    if (control instanceof HTMLElement) {
      const nestedInput = control.querySelector("input");
      return nestedInput instanceof HTMLInputElement ? nestedInput : null;
    }

    return null;
  });

  const defaultVisible = $derived.by(() =>
    resolvedInput ? defaultPasswordFieldVisibility(fieldName || label || labelFor || "", resolvedInput.value || "") : false);
  const visible = $derived(visibleOverride ?? defaultVisible);

  $effect(() => {
    if (!resolvedInput) {
      return;
    }

    resolvedInput.type = visible ? "text" : "password";
  });
</script>

<FormField {label} {description} {labelFor} {wrapperClass} {required}>
  <div class="mqb-password-field">
    <MountedNode node={control} className="mqb-password-field__control" />
    <button
      type="button"
      class="wa-native-button wa-native-button--neutral mqb-password-field__toggle"
      aria-pressed={visible}
      aria-label={visible ? "Mask sensitive value" : "Show sensitive value"}
      onclick={() => {
        visibleOverride = !visible;
      }}
    >
      {visible ? visibleLabel : maskedLabel}
    </button>
  </div>
</FormField>

<style>
  .mqb-password-field {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 0.5rem;
    align-items: center;
  }

  .mqb-password-field :global(.mqb-password-field__control > *),
  .mqb-password-field :global(.mqb-password-field__control input) {
    width: 100%;
  }

  .mqb-password-field__toggle {
    white-space: nowrap;
  }
</style>
