<script lang="ts">
  import ScalarEndpointInput from "./ScalarEndpointInput.svelte";
  import { appShell } from "../../lib/app-shell";
  import { collectPublisherIdSuggestions } from "../endpoint-metadata";

  interface Props {
    title?: string;
    description?: string;
    value: string;
    placeholder?: string;
    name?: string;
    onChange: (next: string) => void;
  }

  let {
    title = "Ref Endpoint",
    description = "",
    value,
    placeholder = "Select a publisher id...",
    name,
    onChange,
  }: Props = $props();

  const suggestions = $derived(
    collectPublisherIdSuggestions(appShell.config<Record<string, any>>()?.publishers),
  );
</script>

<ScalarEndpointInput
  {title}
  {description}
  {value}
  {placeholder}
  {suggestions}
  {name}
  {onChange}
/>
