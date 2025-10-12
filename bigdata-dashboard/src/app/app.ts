import { Component, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { DashboardComponent } from './dashboard/dashboard';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, DashboardComponent],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('bigdata-dashboard');
}
