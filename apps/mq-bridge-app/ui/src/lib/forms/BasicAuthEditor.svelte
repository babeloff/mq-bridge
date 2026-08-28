<script lang="ts">
  interface Props {
    title?: string;
    description?: string;
    username: string;
    password: string;
    onChange: (next: { username: string; password: string }) => void;
  }

  let {
    title = "Basic Auth",
    description = "",
    username,
    password,
    onChange,
  }: Props = $props();
  const uid = $props.id();

  function updateUsername(value: string) {
    username = value;
    onChange({ username, password });
  }

  function updatePassword(value: string) {
    password = value;
    onChange({ username, password });
  }
</script>

<fieldset class="ui_basic_auth mqb-inline-editor">
  <legend>{title}</legend>

  {#if description}
    <div class="form-description form-description-block mqb-form-description-block">{description}</div>
  {/if}

  <div class="wa-form-row mqb-form-row">
    <label class="wa-form-label mqb-form-label" for={`${uid}-username`}>Username</label>
    <div class="wa-form-col mqb-form-col">
      <input
        id={`${uid}-username`}
        class="field-input"
        type="text"
        value={username}
        oninput={(event) => updateUsername((event.currentTarget as HTMLInputElement).value)}
      />
    </div>
  </div>

  <div class="wa-form-row mqb-form-row">
    <label class="wa-form-label mqb-form-label" for={`${uid}-password`}>Password</label>
    <div class="wa-form-col mqb-form-col">
      <input
        id={`${uid}-password`}
        class="field-input"
        type="password"
        value={password}
        oninput={(event) => updatePassword((event.currentTarget as HTMLInputElement).value)}
      />
    </div>
  </div>
</fieldset>
