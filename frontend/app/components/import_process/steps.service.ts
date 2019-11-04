import { Injectable } from '@angular/core';
import { Observable, BehaviorSubject } from 'rxjs';
import { FormGroup } from '@angular/forms';

export interface ICurStep {
    id: 'one'|'two'|'three',
    stepForm: FormGroup,
    type: 'next',
    data: any
}

@Injectable()
export class StepsService {
    private currentStep: BehaviorSubject<ICurStep> = new BehaviorSubject();
    
    constructor() {}

	getStep(): Observable<ICurStep> {
		return this.currentStep.asObservable();
    }
    
	nextStep(form: FormGroup, step: 'one'|'two'|'three', data?: any) {
		switch (step) {
			case 'one': {
                this.currentStep.next({ id: 'one', stepForm: form, type: 'next', data: data });
				break;
			}
			case 'two': {
				this.currentStep.next({ id: 'two', stepForm: form, type: 'next', data: data });
				break;
            }
            case 'three': {
                this.currentStep.next({ id: 'three', stepForm: form, type: 'next', data: data});
                break;
            }
		}
	}

	previousStep() {
		this.currentStep.next({ type: 'previous' });
	}
}
