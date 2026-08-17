import { useCallback, useState } from 'react';
import { createPortal } from 'react-dom';
import {
    Check,
    Cloud,
    Copy,
    ExternalLink,
    HardDrive,
    Laptop,
    Loader2,
    Server,
    ShieldCheck,
} from 'lucide-react';
import {
    deployTargetClaim,
    deployTargetProbe,
    deployTargetSave,
    runnerStage,
    type StagedRunner,
} from '../tauri-bridge';
import { copyText, openExternal } from '../tauri-io';

/**
 * The one question asked on a first run, and the setup that follows if the answer is
 * "a server".
 *
 * Most people who download this are trying it out on their own machine, and making them
 * configure a team before they can draw a pipeline would be the worst thing we could do to
 * them. So local is a single click that changes nothing and never asks again.
 *
 * The server path exists because a Duckle server brought up in a cloud has nobody
 * administering it yet, and the alternative to finishing that here is a shell session on
 * the box. This app is the setup client: it asks the server which of the two it is, and
 * then either claims it or takes a key for one somebody else already claimed.
 */

type Props = {
    workspacePath: string;
    /** Called once the choice is made, with what was chosen. */
    onDone: (choice: 'local' | 'server') => void;
};

type Step = 'choose' | 'where' | 'standup' | 'address' | 'claim' | 'key' | 'done';

type Provider = 'custom' | 'aws' | 'gcp' | 'azure';

const DEPLOY_GUIDE = 'https://duckle.org/deploy.html';

/**
 * How to get a Duckle server running, per place it might run.
 *
 * Short on purpose. The full recipes - load balancers, secrets, autoscaling, the lot -
 * live on the deploy page, and repeating them here would mean two versions of the same
 * instructions drifting apart. What belongs in a wizard is the shortest path to a server
 * that answers, because the next thing this wizard does is claim it.
 *
 * The cloud commands run a container, so nothing has to be uploaded. Only the custom path
 * hands over a binary, and that binary comes out of this app rather than off the network.
 */
const PROVIDERS: Record<
    Provider,
    { name: string; blurb: string; runsOn: string; steps: string[]; command: string }
> = {
    custom: {
        name: 'Custom',
        blurb: 'A machine you run yourself, including this one',
        runsOn: 'Anywhere you can run one file',
        steps: [
            'Put the runner somewhere you can reach it (button below).',
            'Start it, pointing at a folder for the workspace.',
            'Wait for the line that says NOT SET UP.',
        ],
        // 0.0.0.0 rather than 127.0.0.1 is the whole trick: a server bound to loopback with
        // no accounts counts as already yours, answers 410, and can never be claimed.
        command:
            'duckle-runner serve --workspace ./duckle-workspace --host 0.0.0.0 --port 8090',
    },
    aws: {
        name: 'AWS',
        blurb: 'EC2, ECS on Fargate, or EKS',
        runsOn: 'EC2 is the straightforward one',
        steps: [
            'Launch an EC2 instance with Docker, and open port 8080 to yourself only.',
            'Run the container, giving it a folder for the workspace.',
            'Wait for the line that says NOT SET UP.',
        ],
        command:
            'docker run -d --restart=always --name duckle -p 8080:8080 \\\n' +
            '  -v /srv/duckle:/workspace ghcr.io/slothflowlabs/duckle-web:latest \\\n' +
            '  duckle-runner serve --host 0.0.0.0 --port 8080 --workspace /workspace',
    },
    gcp: {
        name: 'Google Cloud',
        blurb: 'Compute Engine, Cloud Run, or GKE',
        runsOn: 'Compute Engine is the straightforward one',
        steps: [
            'Create an instance from the container image.',
            'Allow yourself through the firewall on port 8080.',
            'Wait for the line that says NOT SET UP.',
        ],
        command:
            'gcloud compute instances create-with-container duckle \\\n' +
            '  --machine-type=c3-standard-8 \\\n' +
            '  --container-image=ghcr.io/slothflowlabs/duckle-web:latest \\\n' +
            '  --container-command=duckle-runner \\\n' +
            '  --container-arg=serve --container-arg=--host --container-arg=0.0.0.0 \\\n' +
            '  --container-arg=--port --container-arg=8080',
    },
    azure: {
        name: 'Azure',
        blurb: 'Virtual Machine, Container Apps, or AKS',
        runsOn: 'A VM is the straightforward one',
        steps: [
            'Create a Linux VM with Docker, and restrict inbound to yourself.',
            'Run the container, giving it a folder for the workspace.',
            'Wait for the line that says NOT SET UP.',
        ],
        command:
            'docker run -d --restart=always --name duckle -p 8080:8080 \\\n' +
            '  -v /srv/duckle:/workspace ghcr.io/slothflowlabs/duckle-web:latest \\\n' +
            '  duckle-runner serve --host 0.0.0.0 --port 8080 --workspace /workspace',
    },
};

export default function SetupWizard({ workspacePath, onDone }: Props) {
    const [step, setStep] = useState<Step>('choose');
    const [url, setUrl] = useState('');
    const [name, setName] = useState('production');
    const [admin, setAdmin] = useState('');
    const [apiKey, setApiKey] = useState('');
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [provider, setProvider] = useState<Provider>('custom');
    const [runner, setRunner] = useState<StagedRunner | null>(null);
    const [copied, setCopied] = useState(false);

    const copy = async (text: string) => {
        setCopied(await copyText(text));
        window.setTimeout(() => setCopied(false), 1600);
    };

    // The runner is not fetched, it is unpacked: both binaries are inside this app, so the
    // one handed over always matches this build and setup works with no network.
    const getRunner = useCallback(async () => {
        setBusy(true);
        setError(null);
        try {
            setRunner(await runnerStage(provider === 'custom' ? 'native' : 'linux'));
        } catch (e) {
            setError(e instanceof Error ? e.message : String(e));
        } finally {
            setBusy(false);
        }
    }, [provider]);

    const fail = (e: unknown) => {
        setError(e instanceof Error ? e.message : String(e));
        setBusy(false);
    };

    // Ask the server what it is before asking the person anything. A server nobody has
    // claimed wants a name; one that is already set up wants a key. Guessing wrong means
    // asking for something they do not have yet.
    const probe = useCallback(async () => {
        setBusy(true);
        setError(null);
        try {
            const state = await deployTargetProbe(url);
            setBusy(false);
            setStep(state === 'unclaimed' ? 'claim' : 'key');
        } catch (e) {
            fail(e);
        }
    }, [url]);

    const claim = useCallback(async () => {
        setBusy(true);
        setError(null);
        try {
            await deployTargetClaim(workspacePath, name, url, admin);
            setBusy(false);
            setStep('done');
        } catch (e) {
            fail(e);
        }
    }, [workspacePath, name, url, admin]);

    const saveKey = useCallback(async () => {
        setBusy(true);
        setError(null);
        try {
            await deployTargetSave(workspacePath, name, url, apiKey);
            setBusy(false);
            setStep('done');
        } catch (e) {
            fail(e);
        }
    }, [workspacePath, name, url, apiKey]);

    const body = (
        <div className="modal-backdrop">
            <div className="modal setup-card">
                {step === 'choose' && (
                    <>
                        <h2 className="setup-title">How are you using Duckle?</h2>
                        <p className="setup-sub">
                            You can change this later. Nothing here is permanent.
                        </p>
                        <div className="setup-choices">
                            <button className="setup-choice" onClick={() => onDone('local')}>
                                <Laptop size={22} />
                                <span className="setup-choice-title">Just me, on this machine</span>
                                <span className="setup-choice-sub">
                                    Draw pipelines and run them here. No accounts, no server,
                                    nothing to set up.
                                </span>
                            </button>
                            <button className="setup-choice" onClick={() => setStep('where')}>
                                <Cloud size={22} />
                                <span className="setup-choice-title">My team, on a server</span>
                                <span className="setup-choice-sub">
                                    Author here and deploy to a server you own, where pipelines
                                    run on a schedule.
                                </span>
                            </button>
                        </div>
                    </>
                )}

                {step === 'where' && (
                    <>
                        <h2 className="setup-title">Where will your server run?</h2>
                        <p className="setup-sub">
                            Duckle is one binary, so this only changes the command you run. You
                            own the machine either way.
                        </p>
                        <div className="setup-choices">
                            {(Object.keys(PROVIDERS) as Provider[]).map(id => (
                                <button
                                    key={id}
                                    className="setup-choice"
                                    onClick={() => {
                                        setProvider(id);
                                        setRunner(null);
                                        setError(null);
                                        setStep('standup');
                                    }}
                                >
                                    {id === 'custom' ? <HardDrive size={22} /> : <Cloud size={22} />}
                                    <span className="setup-choice-title">{PROVIDERS[id].name}</span>
                                    <span className="setup-choice-sub">{PROVIDERS[id].blurb}</span>
                                </button>
                            ))}
                        </div>
                        <div className="setup-actions">
                            <button className="setup-back" onClick={() => setStep('choose')}>
                                Back
                            </button>
                        </div>
                    </>
                )}

                {step === 'standup' && (
                    <>
                        <h2 className="setup-title">Start Duckle on {PROVIDERS[provider].name}</h2>
                        <p className="setup-sub">{PROVIDERS[provider].runsOn}. Three steps.</p>

                        <ol className="setup-steps">
                            {PROVIDERS[provider].steps.map(s => (
                                <li key={s}>{s}</li>
                            ))}
                        </ol>

                        {provider === 'custom' ? (
                            <>
                                <button
                                    className="setup-alt setup-getrunner"
                                    onClick={getRunner}
                                    disabled={busy}
                                    type="button"
                                >
                                    {busy ? <Loader2 size={15} className="spin" /> : null}
                                    {runner ? 'Put it there again' : 'Put the runner on this machine'}
                                </button>
                                {runner ? (
                                    <p className="setup-note">
                                        <Check size={14} />
                                        <span>
                                            {runner.platform} runner ready at{' '}
                                            <code>{runner.path}</code>
                                        </span>
                                    </p>
                                ) : null}
                            </>
                        ) : null}

                        <div className="setup-command">
                            <pre>{PROVIDERS[provider].command}</pre>
                            <button
                                className="setup-alt"
                                type="button"
                                onClick={() => copy(PROVIDERS[provider].command)}
                            >
                                <Copy size={13} /> {copied ? 'Copied' : 'Copy'}
                            </button>
                        </div>

                        {/* One element after the icon, not loose text: .setup-note is a flex
                            row, so every inline <code> would otherwise become its own flex
                            item and the sentence would come apart into columns. */}
                        <p className="setup-note">
                            <ShieldCheck size={14} />
                            <span>
                                Bind it to <code>0.0.0.0</code>, not <code>127.0.0.1</code>. A
                                server that only answers on loopback counts as already yours and
                                cannot be claimed from here.
                            </span>
                        </p>

                        {error && <div className="setup-error">{error}</div>}
                        <div className="setup-actions">
                            <button className="setup-back" onClick={() => setStep('where')}>
                                Back
                            </button>
                            <button
                                className="setup-alt"
                                type="button"
                                onClick={() => void openExternal(DEPLOY_GUIDE)}
                            >
                                <ExternalLink size={13} /> Full guide
                            </button>
                            <button className="setup-next" onClick={() => setStep('address')}>
                                It is running
                            </button>
                        </div>
                    </>
                )}

                {step === 'address' && (
                    <>
                        <h2 className="setup-title">Where is your server?</h2>
                        <p className="setup-sub">
                            The address of a machine running <code>duckle-runner serve</code>.
                        </p>
                        <label className="setup-label" htmlFor="setup-url">
                            Address
                        </label>
                        <input
                            id="setup-url"
                            className="setup-input"
                            value={url}
                            onChange={(e) => setUrl(e.target.value)}
                            placeholder="https://duckle.internal"
                            autoFocus
                        />
                        <label className="setup-label" htmlFor="setup-name">
                            Call it
                        </label>
                        <input
                            id="setup-name"
                            className="setup-input"
                            value={name}
                            onChange={(e) => setName(e.target.value)}
                            placeholder="production"
                        />
                        {error && <div className="setup-error">{error}</div>}
                        <div className="setup-actions">
                            <button className="setup-back" onClick={() => setStep('standup')}>
                                Back
                            </button>
                            <button className="setup-next" onClick={probe} disabled={busy || !url.trim()}>
                                {busy ? <Loader2 size={15} className="spin" /> : null}
                                Continue
                            </button>
                        </div>
                    </>
                )}

                {step === 'claim' && (
                    <>
                        <h2 className="setup-title">Nobody administers this server yet</h2>
                        <p className="setup-sub">
                            Put your name in and it becomes yours. You decide who else gets in,
                            and what they can do.
                        </p>
                        <label className="setup-label" htmlFor="setup-admin">
                            Your name
                        </label>
                        <input
                            id="setup-admin"
                            className="setup-input"
                            value={admin}
                            onChange={(e) => setAdmin(e.target.value)}
                            placeholder="e.g. sourav"
                            autoFocus
                        />
                        <p className="setup-note">
                            <ShieldCheck size={14} /> Its key is saved here, encrypted, and never
                            shown again. That is the whole of what this machine keeps.
                        </p>
                        {error && <div className="setup-error">{error}</div>}
                        <div className="setup-actions">
                            <button className="setup-back" onClick={() => setStep('address')}>
                                Back
                            </button>
                            <button
                                className="setup-next"
                                onClick={claim}
                                disabled={busy || !admin.trim()}
                            >
                                {busy ? <Loader2 size={15} className="spin" /> : null}
                                Claim it
                            </button>
                        </div>
                    </>
                )}

                {step === 'key' && (
                    <>
                        <h2 className="setup-title">This server is already set up</h2>
                        <p className="setup-sub">
                            Ask an administrator for a key. In the console that is People, then
                            Create key.
                        </p>
                        <label className="setup-label" htmlFor="setup-key">
                            Key
                        </label>
                        <input
                            id="setup-key"
                            className="setup-input"
                            value={apiKey}
                            onChange={(e) => setApiKey(e.target.value)}
                            placeholder="duckle_..."
                            autoFocus
                        />
                        {error && <div className="setup-error">{error}</div>}
                        <div className="setup-actions">
                            <button className="setup-back" onClick={() => setStep('address')}>
                                Back
                            </button>
                            <button
                                className="setup-next"
                                onClick={saveKey}
                                disabled={busy || !apiKey.trim()}
                            >
                                {busy ? <Loader2 size={15} className="spin" /> : null}
                                Connect
                            </button>
                        </div>
                    </>
                )}

                {step === 'done' && (
                    <>
                        <div className="setup-done-mark">
                            <Check size={26} />
                        </div>
                        <h2 className="setup-title">Connected to {name}</h2>
                        <p className="setup-sub">
                            Build a pipeline here, then deploy it to {name}. Its schedule arrives
                            switched off, so nothing starts running until you say so.
                        </p>
                        <div className="setup-actions">
                            <button className="setup-next" onClick={() => onDone('server')}>
                                <Server size={15} /> Start building
                            </button>
                        </div>
                    </>
                )}
            </div>
        </div>
    );

    return createPortal(body, document.body);
}
