<script lang="ts">
  interface Props {
    title?: string;
    description?: string;
    value: string;
    placeholder?: string;
    suggestions?: string[];
    name?: string;
    onChange: (next: string) => void;
  }

  let {
    title = "Value",
    description = "",
    value,
    placeholder = "",
    suggestions = [],
    name,
    onChange,
  }: Props = $props();

  const uid = $props.id();
</script>

<fieldset class="mqb-inline-editor mqb-scalar-endpoint">
  <legend>{title}</legend>

  {#if description}
    <div class="form-description form-description-block mqb-form-description-block">{description}</div>
  {/if}

  <div class="wa-form-row mqb-form-row">
    <label class="wa-form-label mqb-form-label" for={`${uid}-value`}>{title}</label>
    <div class="wa-form-col mqb-form-col">
      <input
        id={`${uid}-value`}
        class="field-input"
        type="text"
        name={name}
        value={value}
        placeholder={placeholder}
        list={suggestions.length > 0 ? `${uid}-suggestions` : undefined}
        oninput={(event) => onChange((event.currentTarget as HTMLInputElement).value)}
      />
      {#if suggestions.length > 0}
        <datalist id={`${uid}-suggestions`}>
          {#each suggestions as suggestion (suggestion)}
            <option value={suggestion}></option>
          {/each}
        </datalist>
      {/if}
    </div>
  </div>
</fieldset>
