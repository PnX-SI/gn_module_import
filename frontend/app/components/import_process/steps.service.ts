import { Injectable } from '@angular/core';
import { Observable, BehaviorSubject } from 'rxjs';
import { FormGroup } from '@angular/forms';

@Injectable()
export class StepsService {
	private currentStep: BehaviorSubject<any> = new BehaviorSubject();

	constructor() {}

	getStep(): Observable<any> {
		return this.currentStep.asObservable();
	}

	nextStep(form: FormGroup, step: string, data?: any) {
		switch (step) {
			case 'one': {
				this.currentStep.next({ id: 'one', stepForm: form, type: 'next', data: data });
				break;
			}
			case 'two': {
				this.currentStep.next({ id: 'two', stepForm: form, type: 'next', data: data });
				break;
			}
		}
	}

	previousStep() {
		this.currentStep.next({ type: 'previous' });
	}
}
