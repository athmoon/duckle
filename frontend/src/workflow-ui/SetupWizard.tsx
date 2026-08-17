import { useCallback, useState } from 'react';
import { createPortal } from 'react-dom';
import { Check, Cloud, Laptop, Loader2, Server, ShieldCheck } from 'lucide-react';
import { deployTargetClaim, deployTargetProbe, deployTargetSave } from '../tauri-bridge';

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

type Step = 'choose' | 'address' | 'claim' | 'key' | 'done';

export default function SetupWizard({ workspacePath, onDone }: Props) {
    const [step, setStep] = useState<Step>('choose');
    const [url, setUrl] = useState('');
    const [name, setName] = useState('production');
    const [admin, setAdmin] = useState('');
    const [apiKey, setApiKey] = useState('');
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);

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
                            <button className="setup-choice" onClick={() => setStep('address')}>
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
                            <button className="setup-back" onClick={() => setStep('choose')}>
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
